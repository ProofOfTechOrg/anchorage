# Connector interface

A breakwater connector is a Mastra tool whose safety declaration is enforced in the returned tool's execution path.

Use `createConnector()` instead of `createTool()` for code that can reach a network, change state, require human review, need replay protection, or consume a shared budget.

## Basic connector

```typescript
import {
  createConnector,
  InMemoryIdempotencyStore,
  InMemoryRateLimitStore,
} from '@proofoftech/breakwater/connector-sdk';
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
  policies: {
    idempotencyStore: new InMemoryIdempotencyStore(),
    idempotencyKeyMigration: 'legacy-writers-drained',
    rateLimitStore: new InMemoryRateLimitStore(),
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

The in-memory stores make this definition runnable in one process. Use the D1
stores and the physical-deployment preset when replay or budgets must survive
another isolate or restart.

## Invoke a connector directly

Use `invokeConnector()` from trusted host or workflow code. It supplies Mastra's required execution context without asking you to cast a partial object:

```typescript
import { invokeConnector } from '@proofoftech/breakwater/connector-sdk';

const contact = await invokeConnector(createContact, input, {
  requestContext,
  abortSignal,
  toolCallId: trustedToolCallId,
});
```

Omit `toolCallId` for suspension and run grants. For a tool-call grant, pass the exact runtime-owned ID. Never derive it from client input or store it in a shared `RequestContext`.

The helper accepts only an unmodified `Connector` created by `createConnector()`. It calls the public Mastra execution wrapper, so schema validation and every Breakwater gate remain active. Input or output validation throws `ConnectorValidationError`. The error exposes only `connector` and `phase`; it omits Mastra's raw message, schema issue text, invalid value, and cause.

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

The application chooses the business key. Flowsafe does not invent a connector-specific idempotency key or mint an organization isolation scope. Its shared store keys are deployment-wide.

Breakwater stores only `{ result }`; it does not canonicalize arbitrary input or compare request bodies. A gateway that promises mismatch detection owns the canonical request representation and stores its fingerprint with the result. The same key and fingerprint replays, while the same key with a different fingerprint is rejected at that gateway boundary.

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
  idempotencyKeyMigration?: 'legacy-writers-drained';
  rateLimitStore?: RateLimitStore;
  audit?: AuditLogger;
  fetch?: EgressFetchBase;
}
```

The connector definition binds these values once. `fetch` is the underlying HTTP implementation wrapped by guarded fetch, and is the normal test seam.

### Apply the physical-deployment preset

Use `singleTenantConnectorPolicies()` when one physically isolated deployment serves one organization. Given a D1-compatible `db` and an `AuditLogger` with an external sink, construct the shared policy set once:

```typescript
import {
  D1IdempotencyStore,
  D1RateLimitStore,
  singleTenantConnectorPolicies,
} from '@proofoftech/breakwater/connector-sdk';

const policies = singleTenantConnectorPolicies({
  durableStores: {
    idempotency: new D1IdempotencyStore(db),
    rateLimit: new D1RateLimitStore(db),
  },
  idempotencyKeyMigration: 'legacy-writers-drained',
  audit: { mode: 'production', logger: audit },
  egress: { allowedDomains: ['api.example.com'] },
  permissions: { principalPermissions: 'configured' },
});
```

Pass `policies` to every connector in the deployment. The preset enforces these construction-time invariants:

- A manifest that declares idempotency or a rate limit has the matching D1-backed store.
- Production mode has an external audit sink. Only development mode can explicitly opt out of audit.
- Every declared egress host fits the organization allowlist.
- A permission-declaring connector runs only when the host declares principal-permission projection wiring.
- `backgroundExecution()` is installed once, destructive approval cannot be weakened, and `tenantIsolation()` is rejected because connector keys are deployment-wide.

The migration acknowledgement means every legacy connector writer sharing the D1 store has stopped and drained and existing legacy rows have been inventoried. For a new empty deployment, it confirms that no legacy writer exists. Do not set it during a mixed-version rollout.

The returned policy set is frozen. `createConnector()` rejects copied, mutated, or replaced preset members instead of accepting a weakened configuration.

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

A denial throws `ConnectorPolicyError` with connector id, policy name, and reason. Every gate records its decision through the supplied audit logger. Connector decisions use `agentAuditDetail()`, so trusted `breakwater.auditContext` correlation overrides same-named decision detail.

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

- `tool-call`: connector, workflow, run, exact suspension, and Mastra `toolCallId`
- `suspension`: connector, workflow, run, and exact suspension
- `run`: connector, workflow, and run for a trusted standing grant

Each scope can also carry Breakwater's optional opaque `isolationScope` when a trusted non-Flowsafe host defines another logical partition. Flowsafe grants omit it.

Durable-agent approvals use `tool-call` scope because Mastra persists and reproduces `toolCallId` through retry and reconstruction. Workflow gates use `suspension` scope because an arbitrary workflow suspension has no reproducible tool-call identity.

Grant checks run on every attempt. A retry of the same durable tool call keeps the same identity and remains authorized. A new model tool call receives a new ID and requires approval. Use `breakwater.idempotencyKey` when retrying a side effect must replay instead of execute again.

Legacy connector ID arrays and malformed structured grants fail closed. Breakwater does not hash inputs or consume one-shot nonces: canonical serialization, redaction, retry, and atomic consumption semantics are not established at this boundary.

The dry-run branch is the only approval exemption because its separate implementation is required to have no side effect.

## Idempotency

New records use a private versioned key whose tuple components are encoded
without an ambiguous delimiter. Store keys are opaque; applications must not
construct or parse them.

The legacy v1 key was:

```text
[isolationScope:]connectorId:idempotencyKey
```

The connector ID remains colon-free because rate-budget keys retain their
`[isolationScope:]connectorId` shape to preserve active windows. The isolation
prefix appears when the trusted host sets `breakwater.isolationScope`.

Every call probes its exact v1 key before using v2:

- An unscoped key whose business key contains no colon is unambiguous. A completed record replays, and a pending record denies until retry.
- Every scoped key and every unscoped business key containing a colon is ambiguous. A pending or completed v1 row fails with `idempotency-key-migration`; Breakwater never guesses which tuple owns it.
- An absent v1 row can proceed only with `idempotencyKeyMigration: 'legacy-writers-drained'`. This prevents an old writer from creating the legacy row after the new writer inspected it.

To upgrade, stop and drain all old writers that share the store. Set
`idempotencyKeyMigration: 'legacy-writers-drained'` only after that drain.
Inventory legacy rows through the connector-bound helper; it derives the private
v1 key without exposing either storage format:

```typescript
import {
  inspectLegacyConnectorIdempotency,
  migrateLegacyConnectorIdempotency,
} from '@proofoftech/breakwater/connector-sdk';

const identity = {
  idempotencyKey: 'invoice:2026-08-12',
  isolationScope: 'acme',
};
const inventory = await inspectLegacyConnectorIdempotency(connector, identity);
```

Use business or audit evidence to associate each ambiguous row with exactly one
tuple. When that proof exists, pass the exact inventoried record back to the
supported D1 migration:

```typescript
if (inventory.state === 'replay') {
  const result = await migrateLegacyConnectorIdempotency(connector, {
    ...identity,
    expectedRecord: inventory.record,
  });
}
```

The expected record detects a row changed after inventory; it does not itself
prove tuple ownership. The helper requires an ambiguous identity, the
drained-writer acknowledgement, and `D1IdempotencyStore` with transactional
`batch()`. It validates and transforms the legacy output through the connector's
synchronous output schema, guards the exact source value, writes the validated
value under v2, and deletes v1 in one D1 transaction. Store keys remain opaque.

Successful states are `migrated` and idempotent `already-migrated`. Expected
non-mutating outcomes are `source-absent`, `source-pending`, `source-mismatch`,
`target-conflict`, and `output-invalid`. Pending, changed, invalid, conflicting,
and unproven rows remain in place so calls continue to fail closed. Safe legacy
rows remain a read-only replay fallback.

### In-memory store

`InMemoryIdempotencyStore` supports atomic reservation and same-isolate in-flight joining. It is bounded and evictable. It does not protect two Worker isolates or two per-run Durable Objects.

It also exposes non-mutating `inspect()` for migration tests and single-process
upgrades.

### D1 store

`D1IdempotencyStore` claims a key through an atomic insert. A reservation carries a lease token:

- `reserved`: this caller executes;
- `replay`: return the stored result;
- `pending`: another owner still executes.

A stale pending row can be taken over after `pendingTtlMs`. Token-guarded commit and release stop a stale holder from overwriting or deleting the new lease.

The normal `IdempotencyDatabase` seam remains prepare-only. Supported migration
additionally requires the exported `IdempotencyBatchDatabase` shape. Its
`batch()` must provide D1-compatible ordered transaction semantics and roll back
the whole sequence when a statement fails.

New v2 records contain the exact validated/transformed public output. Replays
return that value without rerunning a stateful schema. A safe legacy v1 record
is validated/transformed once on read because it predates the v2 commit
invariant.

Set the TTL above the longest connector or Agent CLI timeout. The constructor
requires a positive safe integer no greater than 8,640,000,000,000,000 ms.

Durable custom stores must implement both `AtomicIdempotencyStore` and
`InspectableIdempotencyStore`. `inspect()` distinguishes absent, pending, and
completed legacy rows without reserving or releasing them. An atomic store
without that capability is rejected at connector construction.

Execution or rate-limit failures before a successful side effect release the
reservation. If output validation fails after execution, Breakwater does not
commit the invalid result and leaves an atomic reservation pending until stale
takeover or operator recovery; releasing it immediately could duplicate the
completed side effect. A successful side effect followed by a failed
result-store write returns the result and audits degraded replay protection;
throwing at that point would likewise invite a duplicate retry.

`D1IdempotencyStore` persists JSON-native results and the established
top-level `undefined` result. Values that JSON would silently change—such as
`Date`, `Map`, repeated object references, non-finite numbers, sparse arrays,
or nested `undefined`—fail the final write before the row changes. The public
result is returned with a degraded-store audit and the atomic reservation
remains pending, so D1 never replays a different type or structure.

## Rate-limit store

`InMemoryRateLimitStore` is per isolate. In a one-Durable-Object-per-run host, that is effectively per run.

`D1RateLimitStore` shares the window across isolates. Flowsafe uses it for deployment-wide enforcement.

The declared count must be a safe integer from 1 through
`Number.MAX_SAFE_INTEGER`. D1 executes its post-increment count and guarded
expired-window cleanup in one transaction. If cleanup fails, the increment
rolls back and execution remains rejected, so the retry does not inherit
phantom quota spend.

The exported structural `RateLimitDatabase` seam requires D1-compatible
transactional `batch()` behavior. A custom adapter must roll back every
statement when any statement in the batch fails.

Both idempotency and rate identities include the isolation scope when present,
but their storage encodings differ. A generic host that requires logical
partitioning can add `tenantIsolation()` so a missing scope is denied. Do not
add it to a Flowsafe connector: the physically isolated runtime intentionally
reserves and omits that scope.

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
9. missing opaque isolation scope when a generic host explicitly requires one;
10. workflow target mismatch where relevant;
11. audit safety for arbitrary throws;
12. packed npm consumer behavior.

For Agent CLIs, also read [Agent CLI connectors](agent-cli-connectors.md).
