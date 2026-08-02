# Deploy Flowsafe on Cloudflare

This reference Worker connects the Flowsafe Durable Object runner and approval
queue on Cloudflare. It uses one Durable Object per run, Cloudflare D1 for
snapshots and approvals, bearer-token authentication, and cron triggers for
service-level agreement (SLA) enforcement and snapshot retention. Copy this
directory into your project as the starting point for a real deployment. It
typechecks in-repo against Flowsafe source through the same
`@proofoftech/flowsafe/*` specifiers you keep when copying.

The spike sibling (`../spike/`) is the minimal worker this template grew from;
deploy differences: real auth, cron maintenance, multi-gate approval
bridging, `/healthz`, and env-tunable SLA/retention.

## Hosting a single tenant

Flowsafe is multi-tenant by construction. You do not turn tenancy off; you
use exactly one tenant. On this template that costs one field per token-map
entry. `@proofoftech/breakwater` is unaffected: a breakwater-only consumer
changes nothing.

**Pick a tenant id.** It must match `^[a-z0-9]{3,32}$` — lowercase letters and
digits only; no underscore, hyphen, or dot. `default` is the conventional
choice and is permanently reserved against allocation, so it can never collide
with a client if you later run more than one tenant; your own company name
(`acme`) is equally fine. The only id you may **not** use is `system` — it is
the audit identity of the cron maintenance actor, and a token carrying it is
rejected at parse time (and re-refused by the tenant resolver, whichever
verifier you plug in).

**Put it in the token map.** Every entry in the `APPROVAL_ACTOR_TOKENS` secret
needs it; an entry without a valid `tenantId` is dropped at parse time (a
`config-error` line in the logs) and its token 401s:

```json
{"tok-ray":{"id":"ray","role":"reviewer","tenantId":"acme"}}
```

That is the whole configuration. Everything downstream binds itself: the
tenant resolver binds each request's approval store to the authenticated
token's tenant, and each run's Durable Object binds its grant store to the
tenant prefix of its own run id. You never write the tenant id into
`worker.ts` — the maintenance actor stays `system` on purpose.

**What changes that you will notice.** Run ids become `acme_<uuid>`. They are
minted server-side; `POST /runs` with a `runId` in the body returns `400`. Do
not parse, pin, or derive anything from a run id — treat it as an opaque
identifier that happens to carry its tenant.

**If you route runs yourself.** `createRunRouter` mints the run id for you. If
you bypass it and call `RunnerRuntime.start` directly, you must still pass
`` `${tenantId}_${uuid}` `` — `start` requires a `runId` and no longer
generates one. This matters because a Durable Object recovers its tenant by
reading the prefix off its own run id: a hand-minted `batch_42` would bind
that Durable Object's grant store to a phantom tenant `batch`, and approvals
created under `acme` would never mint a grant. `ApprovalService` prevents this
state by rejecting approval creation when the run ID does not carry the
tenant's `acme_` prefix.

**What you can ignore.** The `tenants` registry exists to keep multiple
tenants unique; with one tenant you may skip it. Skipping is mandatory for
`default`: it is deliberately not provisionable, which keeps it available for
single-tenant deployments. A named tenant such as `acme` can be provisioned
now to keep the upgrade path clean. The subdomain-to-tenant cross-check
(`TENANT_APEX_DOMAIN`) is for `<client>.example.com` hosts. `hmacVerifier`,
OAuth sign-in, and the demo sandbox are unnecessary for this template.
`staticTokenVerifier` over your token map is the whole identity layer.
`purgeTenant` is for offboarding a client.

**What you must not do.** Do not make `tenantId` optional or give it a
default. An omissible tenant is the one failure this design exists to prevent:
on the day you add a second tenant, every tenant-less token silently becomes
the first one. Typing `"tenantId":"acme"` a few times is the cheaper trade.

## Deploy the Worker

Run these commands from `packages/flowsafe`. Provision every named tenant from
a private control-plane Worker before you issue credentials. The
`provisionTenant()` call creates the registry table and refuses duplicate or
reserved IDs:

```typescript
import { provisionTenant } from '@proofoftech/flowsafe/host-kit';

await provisionTenant(env.DB, {
  tenantId: 'acme',
  kind: 'commercial',
});
```

Single-tenant deployments that use the reserved `default` ID skip this
provisioning call, as described above.

```bash
# 1. Create the database, then paste the printed ID into deploy/wrangler.jsonc.
pnpm exec wrangler d1 create anchorage-flowsafe \
  --config deploy/wrangler.jsonc

# 2. Provision each tenant before issuing any token that names it.
#    provisionTenant() is insert-or-fail against the `tenants` registry;
#    nothing else enforces tenant-id uniqueness, and two clients slugged
#    `acme` would merge their runs, approvals, budgets, and artifacts.
#    tenantId must match ^[a-z0-9]{3,32}$ and must not be a reserved slug.

# 3. Mint actor tokens (any random strings) and store the map as a secret:
#    roles: admin | builder | operator | reviewer | viewer
pnpm exec wrangler secret put APPROVAL_ACTOR_TOKENS \
  --config deploy/wrangler.jsonc
# paste, e.g.: {"tok-ray":{"id":"ray","role":"reviewer","tenantId":"acme"},
#               "tok-op":{"id":"op","role":"operator","tenantId":"acme"}}
# A missing, malformed, or reserved tenantId drops that entry; its token 401s.

# 4. Optional: enable live WebSocket streams with a dedicated signing secret.
pnpm exec wrangler secret put STREAM_TICKET_SECRET \
  --config deploy/wrangler.jsonc

# 5. Deploy.
pnpm deploy:cf

# 6. Verify. Copy approval_id and run_id from the start response.
worker_url=https://your_worker_subdomain.workers.dev
operator_token=tok-op
reviewer_token=tok-ray
approval_id=replace_with_approval_id
run_id=replace_with_run_id

curl -fsS "${worker_url}/healthz"
curl -sS -X POST "${worker_url}/runs" \
  -H "authorization: Bearer ${operator_token}" \
  -H 'content-type: application/json' \
  -d '{"workflowId":"example-approval","inputData":{"topic":"launch"}}'
curl -sS "${worker_url}/api/approvals" \
  -H "authorization: Bearer ${reviewer_token}"
curl -sS -X POST \
  "${worker_url}/api/approvals/${approval_id}/decide" \
  -H "authorization: Bearer ${reviewer_token}" \
  -H 'content-type: application/json' \
  -d '{"decision":"approve"}'
curl -sS "${worker_url}/runs/example-approval/${run_id}" \
  -H "authorization: Bearer ${reviewer_token}"
```

For local iteration, run `pnpm deploy:dev`. Wrangler uses local D1 and Durable
Object state, so this command does not require a Cloudflare account.

## Use the HTTP routes

The reference Worker mounts health, workflow, approval, and optional live-stream
routes. Every non-stream route except `GET /healthz` requires a valid bearer
token. `POST /api/stream/ticket` also requires a bearer token; the two
WebSocket routes authenticate with the resulting short-lived ticket.

| Method and route | Purpose |
| --- | --- |
| `GET /healthz` | Return Worker liveness |
| `GET /workflows` | List registered workflows and the authenticated actor |
| `POST /runs` | Start a server-ID-assigned run and queue every observed suspension |
| `GET /runs/:workflowId/:runId` | Read run status and reconcile a missing approval record |
| `POST /runs/:workflowId/:runId/resume` | Resume without adding connector grants; approval-gated connectors still fail closed |
| `GET /api/approvals` | List and filter the tenant's approval queue |
| `GET /api/approvals/metrics` | Read tenant-scoped queue metrics |
| `GET /api/approvals/:id` | Read one approval |
| `POST /api/approvals/:id/claim` | Claim a pending or escalated approval |
| `POST /api/approvals/:id/decide` | Approve or reject one approval and attempt to resume its run |
| `POST /api/approvals/:id/delegate` | Assign an open approval to another reviewer |
| `POST /api/approvals/batch/decide` | Apply one decision to at most 100 unique approval IDs |
| `POST /api/stream/ticket` | Mint a 60-second hub or run stream ticket when streaming is enabled |
| `GET /api/stream/hub?ticket=stream_ticket` | Open the tenant approval and presence WebSocket |
| `GET /api/stream/run/:workflowId/:runId?ticket=stream_ticket` | Open the run-progress WebSocket |

The Worker does not mount `POST /api/approvals`. Approval records come from
observed workflow suspensions, not request bodies. The `/api/stream/*` routes
exist only when both the `HUB` binding and `STREAM_TICKET_SECRET` are present.

## Understand scheduled maintenance

`triggers.crons` declares two expressions, and `scheduled()` dispatches on
`controller.cron` (`SWEEP_CRON` versus `PURGE_CRON` in `crons.ts`) so the two
maintenance duties never share an invocation. A Workers CPU-limit termination
kills the isolate and is not a catchable JavaScript error, so a slow sweep
sharing an invocation could starve the purge. An unrecognized cron expression
runs both duties sequentially and logs a `config-error`, preserving both duties
when Wrangler configuration and the constants disagree.

- **SLA sweep**: The standalone `sweepSLA(factory.system(), …)` escalates
  every open approval past its `slaDeadlineAt`, across all tenants; each
  escalation lands a structured `sla-escalation` log line and the audit
  trail (extend `runSlaSweepMaintenance`'s seam to page/Slack from it).
  It is deliberately **not** a service method and **not** an HTTP route: an
  unfiltered cross-tenant read *and write* behind a role check would let any
  sweep-capable actor escalate every tenant's queue. Its parameter type,
  `SystemApprovalStore`, is unobtainable from a request handler.
- **Run retention purge**: `purgeExpiredWorkflowRuns()` deletes terminal-status
  run snapshots older than `RUN_RETENTION_DAYS` in limited batches per firing.
  The shrinking eligible set acts as the cursor. Suspended and running runs
  are never purged, so a run abandoned at an approval gate is reclaimed only
  by `purgeTenant()` at offboarding. Storing run artifacts in R2? Pass your
  `R2ArtifactStore` as `artifactStore` (here and to `purgeTenant`): the
  snapshot row is the only record of a run's artifact keys, so an unpaired
  retention purge strands the purged runs' artifacts.
- **Thread retention purge**: `purgeExpiredThreads()` deletes agent-memory
  threads whose `updatedAt` is older than `THREAD_RETENTION_DAYS`, each with
  its messages (a message has a `createdAt` but no `updatedAt`, so there is no
  per-message idleness signal — its lifetime is its thread's, and it is
  reachable only through it). A thread is deleted only when no message points
  at it, so a send racing the purge can never be orphaned. The purge uses
  limited batches and the same shrinking-eligible-set cursor. It is unset by
  default: unlike a terminal run snapshot, a conversation is not finished by
  definition, so nothing expires until you name a number.
  Working-memory rows (`mastra_resources`) are never touched here — they
  belong to the owner across every thread, and go at offboarding.
- **Approval retention purge**: `purgeExpiredApprovals()` deletes decided
  (`approved`/`rejected`) approval records whose terminal timestamp
  (`decidedAt`, or `updatedAt` for a decided record persisted without one)
  is older than `APPROVAL_RETENTION_DAYS`. It uses limited batches and the same
  shrinking-eligible-set cursor. Open requests
  (`pending`/`claimed`/`escalated`) are never purged at any age — an
  approval still awaiting a decision is not garbage, mirroring
  `purgeExpiredWorkflowRuns`'s "live runs are never purged". Runs in the
  same `PURGE_CRON` firing as the snapshot purge, in its own isolated
  try/catch, so a failure in either never stops the other.

Keep the sweep interval at or below your SLA granularity; the default
`*/15 * * * *` adds less than 15 minutes of scheduling delay to a 4-hour SLA.
Keep the Wrangler
expressions byte-equal to crons.ts's `SWEEP_CRON`/`PURGE_CRON` constants.

## Configure the deployment

| Name | Kind | Default | Meaning |
| ---- | ---- | ------- | ------- |
| `APPROVAL_ACTOR_TOKENS` | secret | none; protected routes return 401 | JSON map of bearer token → `{ id, role, tenantId }`; entries with a missing or malformed tenant ID, or the reserved `system` ID, are dropped |
| `TENANT_APEX_DOMAIN` | var | unset (no cross-check) | Client-per-subdomain apex, e.g. `example.com`. A request to `<tenant>.<apex>` is denied unless the token's verified tenant is that tenant. Defense in depth over the tenant-bound stores |
| `APPROVAL_SLA_SECONDS` | var | `14400` (4 hours) | Default SLA applied to new approvals |
| `APPROVAL_ALLOW_SELF_DECISION` | var | unset; separation of duties enforced | Exempts decider roles from both requester self-approval and prior-gate reviewer checks. `true` exempts every decider; a comma-separated role list such as `admin` exempts only those roles. Invalid values preserve separation of duties. Permitted requester self-decisions include `detail.selfDecision: true` in audit events |
| `RUN_RETENTION_DAYS` | var | `30` | Terminal snapshot age before cron purge; `0` makes every terminal run eligible on the next purge |
| `APPROVAL_RETENTION_DAYS` | var | `30` | Decided approval age before cron purge; `0` makes every decided record eligible on the next purge |
| `THREAD_RETENTION_DAYS` | var | unset (threads never expire) | Agent-memory thread TTL: idle days before the purge cron deletes a thread and its messages. Unset leaves the duty unwired |
| `STREAM_TICKET_SECRET` | secret | unset (polling only) | Dedicated HMAC key that enables 60-second WebSocket tickets. Keep it distinct from authentication secrets |
| `AUDIT_QUEUE` | queue binding | unbound (logs only) | Enables audit export: events flow to the queue consumer |
| `SIEM_ENDPOINT` | var | none (consumer retries) | HTTP event-collector URL that receives newline-delimited JSON batches |
| `SIEM_AUTH_HEADER` | secret | none | Sent as the `authorization` header on export POSTs |

## The conventions the template encodes

- **Reuse `@proofoftech/flowsafe/host-kit`**: The whole Worker pipeline is
  `createFlowsafeWorker()`: the `/healthz` → approvals → runs → 404 fetch order,
  the authentication seam
  (`parseActorTokens` + `bearerActorAuthenticator`), the run routes with
  their role-based access control (RBAC) order (`createRunRouter`), the
  approval bridge
  (`queueApprovalForSuspension`, `resumeRunWithRequeue`), the two-cron
  `scheduled()` dispatch, and the audit-export `queue()` consumer. These are
  security-critical and tested in the library. `worker.ts` supplies only what
  is deployment-specific: the workflows, the memoized `buildVerifier`, the
  cron expressions, and the optional subdomain cross-check (`wrapResolve`).
  Do not re-derive them.
- **Authenticate first, then construct**: The routers take a `TenantResolver`,
  not a bare `authenticate`: it verifies the token, validates the tenant claim,
  and binds the approval store to that tenant — so there is no pre-auth service
  for a later refactor to reach for. Swap `staticTokenVerifier` for
  `hmacVerifier` or your own `TokenVerifier` (JWKS, OIDC); everything else
  (role checks, separation of duties, self-approval denial, the tenant
  predicates) stays. With no `APPROVAL_ACTOR_TOKENS` secret the map is empty
  and every authenticated route 401s — fail closed.
- **Scope the store factory to the isolate**:
  `D1ApprovalStoreFactory` owns one memoized schema-init pass; rebuilding it
  inside `fetch()` re-runs the whole DDL on every request. The template holds
  it in a module-scoped `WeakMap` keyed by the D1 binding.
- **Provision tenant IDs**: Provision through the `tenants`
  registry before issuing tokens, and read a tenant's *kind* from that registry
  rather than inferring it from its id.
- **Keep approval creation off the public API**: `createApprovalRouter` mounts
  `POST /api/approvals` only when passed `allowCreate: true`, and even then a
  body may not set `connectors` (which *is* the minted grant), `requestedBy`
  (which separation-of-duties compares), or the fields selecting which leg a
  grant mints on. Approval records are authored in-process from an observed
  suspension. Never widen this.
- **Treat each suspension as an approval request**: Both bridges, start and
  resume, run through `queueApprovalForSuspension()`, which captures the suspension's
  `(suspendedAt, resumeCount)` pair so grant minting binds the decision to that
  exact suspension (clock-free), and reads the suspend payload's `connectors`
  array so a decision mints exactly the grants the step asked for. That array
  must be a **server-authored static literal** — deriving it from run input
  would let client input choose its own capability. Multi-gate workflows
  re-enter the queue automatically on each re-suspension
  (`resumeRunWithRequeue`).
- **Keep workflow metadata aligned with runtime registration**: Each entry in `WORKFLOWS`
  is served at `GET /workflows` and gates `POST /runs` via its optional
  `allowedRoles` (a subset of the coarse `RUN_START_ROLES` check that runs
  first). `defineWorkflows` asserts every listed id was actually committed.
- **Derive grants on the server**: The Durable Object-side
  `approvalGrantProvider` derives `breakwater.connectorGrants` from `approved`
  records on every start or resume. `RunnerRuntime` derives
  `breakwater.connectorExecution` from the authoritative leg. The public
  raw-resume route stays grant-free and gated steps fail closed on forged
  resumes. A forged `resumeData.approved` can cosmetically flip a workflow
  boolean but grants no connector capability. The side-effecting step
  re-checks the server-derived structured grant, so treat `resumeData` as
  untrusted, never as the security boundary.
- **Preserve separation of duties across gates**: `queueApprovalForSuspension`
  attributes each auto-queued approval to the human who advanced the run
  (`requestedBy` = the starting actor, or the reviewer whose decision caused
  a re-suspension), not the `SYSTEM_ACTOR` bridge — so the library's
  self-decision check can fire and a start actor cannot approve their own
  run. Dropping `requestedBy` here silently disables that control.
- **Emit structured logs and optionally export them**: Audit events, SLA
  escalations, config errors, and maintenance counts all go to Workers Logs
  as single-line JSON (for example, `{"type":"audit"}`). Uncomment
  the `queues` block in wrangler.jsonc and audit events additionally flow
  producer → queue → the `queue` consumer → an authenticated
  newline-delimited JSON POST to
  `SIEM_ENDPOINT` (`@proofoftech/flowsafe/audit-export`); a failed export
  retries the whole batch into Queues backoff and the dead-letter queue, so
  no event is acked before the collector confirmed it.

## Test and verify the deployment

The template's correctness rests on four independently-maintained layers:

1. **Its own executable test.** `worker.e2e.test.ts` (part of `pnpm -r test`)
   drives the real exported handler in-process: real Mastra `D1Store` and
   `D1ApprovalStore` over `node:sqlite` behind a D1-shaped adapter, real
   `FlowsafeRunner` instances behind a stub Durable Object namespace. It pins
   the authentication seam, including the reserved-`system` token drop; the
   full start → auto-queued approval → separation-of-duties denials → reviewer
   decision → grant-minted publish loop; the fail-closed forged resume; the tenant
   boundary (client runId 400, cross-tenant 404s), `scheduled()` (SLA
   escalation + terminal-only retention purge, with the two surfaces isolated
   from each other's failures), and the `queue()` audit-export consumer.
2. **Typecheck.** `pnpm --filter @proofoftech/flowsafe typecheck` includes
   `deploy/tsconfig.json`, so the wiring is type-checked against flowsafe
   source through the real `@proofoftech/flowsafe/*` specifiers.
3. **The library test suite.** Every enforcement rule the template relies on
   — role authorization, compare-and-swap transitions, SLA/escalation, self-decision
   separation of duties, grant derivation — is covered by the `approval-api`
   and `do-runner` unit tests. The template only *feeds* those guarantees; it
   does not reimplement them.
4. **The `spike:verify` end-to-end proof on workerd.** The sibling `../spike/`
   worker shares the same library and is driven by
   `pnpm --filter @proofoftech/flowsafe spike:verify` (also run in CI), which
   proves suspend → process-kill → restart-on-persisted-state → grant-minted
   resume on the real Workers runtime — the process-death durability layer 1
   cannot exercise in-process. A local `pnpm deploy:dev` smoke against
   workerd (no Cloudflare account) covers the same ground for this template
   via the `curl` commands in the deploy checklist above.
