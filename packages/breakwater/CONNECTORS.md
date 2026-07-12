# Authoring connectors

How to build, test, and contribute a breakwater connector. A connector is a
Mastra tool created through `createConnector()` instead of `createTool()`:
the same tool contract, plus a **permission manifest that is enforced at
runtime**, not advisory — the wrapper gates every execution path (agent
calls, workflow calls, nested calls, direct calls) before your `execute`
runs. The authoritative enforcement contract is
[`docs/connector-interface.md`](../../docs/connector-interface.md);
this guide is the authoring walkthrough.

## Anatomy

```typescript
import { createConnector } from '@proofoftech/breakwater/connector-sdk';
import { z } from 'zod';

export function createSlackPoster(webhookHost = 'hooks.slack.com') {
  return createConnector({
    // Dotted namespace, vendor first. Ids are matched by write-approval
    // globs ('slack.*'), so keep them stable across versions.
    id: 'slack.post-message',
    description: 'Post a message to a Slack channel via an incoming webhook',
    inputSchema: z.object({
      webhookPath: z.string().startsWith('/services/'),
      text: z.string().min(1),
    }),
    outputSchema: z.object({ delivered: z.boolean(), simulated: z.boolean().optional() }),
    permissions: {
      // Honest classification is the whole point — see the table below.
      sideEffect: 'write',
      // Every host you call. The egress gate denies anything else.
      egress: [webhookHost],
      // Callers must supply a per-call idempotency key; replays return the
      // stored result without re-posting.
      idempotencyKey: true,
      // Simulation supported — requires dryRunExecute below.
      dryRun: true,
      // Fixed-window budget; only actual executions consume it.
      rateLimit: '60/min',
    },
    // The third argument is the per-call runtime: its fetch is bound to the
    // egress list above — an actual request (redirect hops included) to any
    // other host is denied before bytes leave. Make every network call
    // through it.
    execute: async (input, _context, runtime) => {
      const response = await runtime.fetch(`https://${webhookHost}${input.webhookPath}`, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ text: input.text }),
      });
      if (!response.ok) throw new Error(`slack webhook ${response.status}`);
      return { delivered: true };
    },
    // Same output shape, zero side effects — validate what you can without
    // sending. Callers request it per call (DRY_RUN_CONTEXT_KEY). Gets the
    // same egress-guarded runtime as execute.
    dryRunExecute: async () => ({ delivered: false, simulated: true }),
  });
}
```

## The manifest, field by field

| Field | Meaning | Enforcement |
| ----- | ------- | ----------- |
| `sideEffect` | `'read'` — no state change. `'write'` — creates/updates state. `'destructive'` — deletes or irreversibly changes state. `'idempotent'` — write-class but safely repeatable. | Write-class calls (`write`/`destructive`/`idempotent`) are what the approval gate and `writePermissions` globs match; `destructive` requires approval by default under any `writePermissions` policy. Also surfaces as Mastra tool hints (`readOnlyHint`, `destructiveHint`). |
| `egress` | Every hostname your connector contacts. | Enforced at two levels. Pre-execute, the network-egress gate checks the DECLARED list against the org allowlist (`ConnectorPolicyError`, `policy: 'network-egress'`). At runtime, the `runtime.fetch` guard handed to `execute`/`dryRunExecute` pins the connector's ACTUAL requests — redirect hops included — to the declared list (`policy: 'egress-fetch'`; no declaration = no network). Both audited; deny beats allow. |
| `idempotencyKey` | Caller must set `IDEMPOTENCY_KEY_CONTEXT_KEY` per call. | Replays of a stored key return the stored result without re-executing; concurrent same-key calls join one in-flight execution (atomic reserve when the store supports it). Keys are segmented by the host-minted isolation scope (`ISOLATION_SCOPE_CONTEXT_KEY`) when present — never hand-prefix a tenant into the key. |
| `requiresApproval` | Always require a human approval, regardless of org policy. | The call is denied unless the requestContext grant (`breakwater.approvedConnectors`) names this connector id. The grant is the **only** approval token — an agent-shaped execution context never bypasses it. flowsafe's approval queue mints grants from APPROVED records at resume time. |
| `dryRun` | Connector supports side-effect-free simulation. | Requires `dryRunExecute` (and is forbidden without it). A caller sets `DRY_RUN_CONTEXT_KEY` and gets the simulation — allowed without a grant, because a simulation never reaches a side effect; the real call still needs one. |
| `rateLimit` | `'<count>/<unit>'` — e.g. `'100/min'`, `'10/hour'`. | Fixed windows against `policies.rateLimitStore` (required when declared). Denied calls, replays, and dry-runs don't consume budget. Budgets are segmented per isolation scope when a host mints one, so one tenant cannot exhaust another's window. The store's reach IS the budget's reach: `InMemoryRateLimitStore` caps per isolate — per RUN under flowsafe's DO-per-run routing — so a cap that must hold across isolates needs `D1RateLimitStore` (or an equivalent shared store). |

`policies` (all optional) wires the enforcement environment: `networkEgress`
options, `writePermissions` (org-level approval globs), custom `evaluators`
(run pre-execute, after the egress gate), `idempotencyStore` /
`rateLimitStore` (in-memory and D1 implementations ship), and `audit`
(an `AuditLogger`; every allow/deny/error is recorded).

## Known limits (accepted, not bugs)

- **The egress guard covers `runtime.fetch`, not the raw socket.** Requests
  made through the runtime-injected fetch are enforced per actual request,
  redirect hops included, with credential headers stripped on cross-origin
  hops. What it cannot see is traffic that never goes through it: a vendor
  SDK carrying its own HTTP stack, or code reaching for the global `fetch`.
  Route SDK traffic through `runtime.fetch` (most SDKs accept a
  fetch/transport option) — a connector that fetches around the runtime
  degrades to declaration-only enforcement for that traffic. Denying the
  isolate's raw network (socket-level interception) remains host
  infrastructure, not library, territory.
- **`rateLimit` is a fixed window, not a hard cap.** A burst straddling the
  boundary between two adjacent windows can admit up to ~2x the declared
  budget (worse across isolates under clock skew). This is inherent to fixed
  windows and is accepted; a hard per-second cap needs a token-bucket/GCRA
  store instead of `RateLimitStore`'s fixed-window contract.

## Rules that keep the manifest honest

- **Classify by the worst thing the connector can do.** A "create or
  replace" API is `destructive`, not `write`. When in doubt, go stricter.
- **Make every network call through `runtime.fetch`.** It is the enforcement
  point: requests through it are checked against your declared egress per
  call; requests around it are not checked at all. Vendor SDKs almost always
  accept a custom fetch/transport — pass `runtime.fetch` in.
- **Declare all egress, including redirects and regional hosts.** An
  undeclared host is a denial in production — `runtime.fetch` denies actual
  requests (redirect hops included) to hosts you forgot to declare — so
  missing declarations surface in your own testing. That is by design.
- **`dryRunExecute` must be genuinely side-effect-free.** It runs without an
  approval grant. If your simulation calls the vendor (e.g. a validate
  endpoint), that call must be read-only and its host declared in `egress`.
- **Never read approval state from anywhere but the wrapper.** Do not
  inspect the requestContext for grants inside `execute` — enforcement
  already happened; re-implementing it invites drift.
- **Never derive an idempotency key from something two tenants can share
  without saying so.** The wrapper prefixes the key with the caller's
  `breakwater.isolationScope` when a host mints one, so a cross-run business
  key (`send-email:bob@acme.com`) is safe. Do not defeat that by embedding your
  own tenant discriminator, and do not assume a scope exists — a single-tenant
  host mints none. Absent scope deliberately falls back to shared single-tenant
  keys, so a multi-tenant host must also register the `tenantIsolation`
  evaluator, which denies any scope-less call (dry-run included).
- **No secrets in the connector.** Take credentials via constructor
  parameters or the execution environment; never bake defaults, never log
  them, never put them in audit `detail`.

## Testing expectations

Ship tests with the connector — mock the vendor (inject the mock as
`policies.fetch`, the base fetch the egress guard wraps), never the wrapper. The enforcement paths worth pinning, in the repo's
`#given/#when/#then` style (see
`src/agent-cli/agent-cli.test.ts` for a complete example of testing a
connector through the wrapper):

1. **Happy path** — declared host, grant present (if approval-gated),
   executes and returns the schema'd output.
2. **Egress denial** — both flavors: a manifest declaring a host outside the
   org allowlist is denied pre-execute (`policy: 'network-egress'`), and a
   `runtime.fetch` call to an undeclared host is denied in-flight
   (`policy: 'egress-fetch'`) with your vendor mock (injected as
   `policies.fetch`) never called.
3. **Approval denial** — write-class call without the grant is denied;
   with the grant it executes.
4. **Dry-run** — `DRY_RUN_CONTEXT_KEY` returns the simulation, the vendor
   mock is never called.
5. **Idempotent replay** (if declared) — same key twice executes once and
   returns the stored result.

## Contributing a connector

1. Fork, branch, and build it under `packages/breakwater/src/` following
   [`CONTRIBUTING.md`](../../CONTRIBUTING.md). Community connectors live in
   their own module directory with an `index.ts`, tests, and a `CLAUDE.md`
   file table (match `src/agent-cli/`).
2. Run the full gate locally:
   `pnpm lint && pnpm typecheck && pnpm test && pnpm build`.
3. Open a PR describing the vendor surface, the chosen `sideEffect`
   classification (and why), and the egress list. The review focuses on
   manifest honesty first, code second.

For CLIs rather than HTTP APIs, prefer wrapping
`createAgentCliConnector` (`@proofoftech/breakwater/agent-cli`) — it already
carries the write-class manifest, dry-run preview, timeout, and no-shell
spawn discipline. Your `buildFlags(input)` must return **option flags only,
never the prompt**: the wrapper appends `'--', input.prompt` so a
caller-controlled prompt is always the trailing positional behind an
end-of-options separator (it can never be parsed as a flag). Embedding
`input.prompt` in your own returned array ahead of that tail defeats the
protection for your connector — put option *values* in `--flag=value` form
for the same reason.
