# flowsafe reference deployment

Production-shaped Worker wiring the flowsafe DO runner and approval queue on
Cloudflare: one Durable Object per run, D1 for snapshots and approvals, a
bearer-token auth seam, and a cron trigger that owns SLA enforcement and
snapshot retention. Copy this directory into your project as the starting
point for a real deployment; it typechecks in-repo against flowsafe source
through the same `@proofoftech/flowsafe/*` specifiers you keep when copying.

The spike sibling (`../spike/`) is the minimal worker this template grew from;
deploy differences: real auth, cron maintenance, multi-gate approval
bridging, `/healthz`, and env-tunable SLA/retention.

## Hosting a single tenant

flowsafe is multi-tenant by construction — you do not turn tenancy off, you
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
that DO's grant store to a phantom tenant `batch`, and your approvals (created
under `acme`) would never mint a grant. You will not have to debug that:
`ApprovalService` rejects an approval whose run id does not carry its own
tenant's prefix, loudly, at creation time —

> `runId 'batch_42' does not carry this tenant's prefix 'acme_' — approvals bind to tenant-salted runs (INV-1)`

**What you can ignore.** The `tenants` registry (checklist step 2) exists to
keep multiple tenants unique; with one tenant you may skip it. Skipping is
mandatory for `default` — it is deliberately not provisionable, which is
exactly what keeps it yours; a named tenant like `acme` can be provisioned now
to keep the upgrade path clean. The subdomain↔tenant cross-check
(`TENANT_APEX_DOMAIN`) is for `<client>.example.com` hosts. `hmacVerifier`,
OAuth sign-in, and the demo sandbox are the public demo's problem, not yours —
`staticTokenVerifier` over your token map is the whole identity layer.
`purgeTenant` is for offboarding a client.

**What you must not do.** Do not make `tenantId` optional or give it a
default. An omissible tenant is the one failure this design exists to prevent:
on the day you add a second tenant, every tenant-less token silently becomes
the first one. Typing `"tenantId":"acme"` a few times is the cheaper trade.

## Deploy checklist

```bash
# 1. Create the database; paste the printed id into wrangler.jsonc.
wrangler d1 create anchorage-flowsafe

# 2. Provision each tenant BEFORE issuing any token that names it.
#    provisionTenant() is insert-or-fail against the `tenants` registry;
#    nothing else enforces tenant-id uniqueness, and two clients slugged
#    `acme` would merge their runs, approvals, budgets, and artifacts.
#    tenantId must match ^[a-z0-9]{3,32}$ and must not be a reserved slug.

# 3. Mint actor tokens (any random strings) and store the map as a secret:
#    roles: admin | builder | operator | reviewer | viewer
wrangler secret put APPROVAL_ACTOR_TOKENS --config deploy/wrangler.jsonc
# paste, e.g.: {"tok-ray":{"id":"ray","role":"reviewer","tenantId":"acme"},
#               "tok-op":{"id":"op","role":"operator","tenantId":"acme"}}
# An entry without an INV-3-valid tenantId is DROPPED and its token 401s.

# 4. Deploy (from packages/flowsafe/)
pnpm deploy:cf

# 5. Verify
curl https://<worker>/healthz
curl -X POST https://<worker>/runs \
  -H 'authorization: Bearer tok-op' -H 'content-type: application/json' \
  -d '{"workflowId":"example-approval","inputData":{"topic":"launch"}}'
curl https://<worker>/api/approvals -H 'authorization: Bearer tok-ray'
curl -X POST https://<worker>/api/approvals/<id>/decide \
  -H 'authorization: Bearer tok-ray' -H 'content-type: application/json' \
  -d '{"decision":"approve"}'
curl https://<worker>/runs/example-approval/<runId> \
  -H 'authorization: Bearer tok-ray'
```

Local iteration: `pnpm deploy:dev` (wrangler dev against local D1/DO state —
no Cloudflare account needed).

## What the cron does

`triggers.crons` declares TWO expressions, and `scheduled()` dispatches on
`controller.cron` (`SWEEP_CRON` vs `PURGE_CRON` in crons.ts) so the two
enforcement surfaces never share an invocation — a Workers CPU-limit
termination kills the isolate and is **not** a catchable JS error, so a slow
sweep sharing an invocation would permanently starve the purge no matter how
many try/catches wrap them. (An expression the dispatch does not recognize —
wrangler edited without the constants — runs both sequentially and logs a
config-error: availability of both duties beats purity on a misconfig.)

- **SLA sweep** — the standalone `sweepSLA(factory.system(), …)` escalates
  every open approval past its `slaDeadlineAt`, across all tenants; each
  escalation lands a structured `sla-escalation` log line and the audit
  trail (extend `runSlaSweepMaintenance`'s seam to page/Slack from it).
  It is deliberately **not** a service method and **not** an HTTP route: an
  unfiltered cross-tenant read *and write* behind a role check would let any
  sweep-capable actor escalate every tenant's queue. Its parameter type,
  `SystemApprovalStore`, is unobtainable from a request handler.
- **Retention purge** — `purgeExpiredWorkflowRuns()` deletes terminal-status
  run snapshots older than `RUN_RETENTION_DAYS`, LIMIT-batched per firing
  (the shrinking eligible set is the cursor). Suspended and running runs
  are never purged, so a run abandoned at an approval gate is reclaimed only
  by `purgeTenant()` at offboarding. Storing run artifacts in R2? Pass your
  `R2ArtifactStore` as `artifactStore` (here and to `purgeTenant`): the
  snapshot row is the only record of a run's artifact keys, so an unpaired
  retention purge strands the purged runs' artifacts.
- **Approval retention purge** — `purgeExpiredApprovals()` deletes DECIDED
  (`approved`/`rejected`) approval records whose terminal timestamp
  (`decidedAt`, or `updatedAt` for a decided record persisted without one)
  is older than `APPROVAL_RETENTION_DAYS`, LIMIT-batched per firing, same
  shrinking-eligible-set cursor convention. Open requests
  (`pending`/`claimed`/`escalated`) are never purged at any age — an
  approval still awaiting a decision is not garbage, mirroring
  `purgeExpiredWorkflowRuns`'s "live runs are never purged". Runs in the
  same `PURGE_CRON` firing as the snapshot purge, in its own isolated
  try/catch, so a failure in either never stops the other.

Keep the sweep interval at or below your SLA granularity; the default
`*/15 * * * *` gives 4-hour SLAs minute-scale slack. Keep the wrangler
expressions byte-equal to crons.ts's `SWEEP_CRON`/`PURGE_CRON` constants.

## Configuration

| Name | Kind | Default | Meaning |
| ---- | ---- | ------- | ------- |
| `APPROVAL_ACTOR_TOKENS` | secret | none (all authed routes 401) | JSON map of bearer token → `{ id, role, tenantId }`; entries without an INV-3-valid `tenantId` are dropped |
| `TENANT_APEX_DOMAIN` | var | unset (no cross-check) | Client-per-subdomain apex, e.g. `example.com`. A request to `<tenant>.<apex>` is denied unless the token's verified tenant is that tenant. Defense in depth over the tenant-bound stores |
| `APPROVAL_SLA_SECONDS` | var | `14400` (4h) | Default SLA applied to new approvals |
| `APPROVAL_ALLOW_SELF_DECISION` | var | unset (SoD ON) | Separation-of-duties exemption. Unset or a `false` spelling keeps SoD on; `true` lets any decider self-decide; a CSV of roles (e.g. `admin`) exempts only those — a single-operator deployment sets `admin`. Any invalid value falls back to OFF. Permitted self-decisions are audited (`detail.selfDecision: true`) |
| `RUN_RETENTION_DAYS` | var | `30` | Terminal snapshot age before cron purge; `0` purges terminal runs immediately |
| `APPROVAL_RETENTION_DAYS` | var | `30` | Decided (approved/rejected) approval record age before cron purge; `0` purges decided approvals immediately |
| `AUDIT_QUEUE` | queue binding | unbound (logs only) | Enables audit export: events flow to the queue consumer |
| `SIEM_ENDPOINT` | var | none (consumer retries) | HTTP event-collector URL the consumer POSTs NDJSON batches to |
| `SIEM_AUTH_HEADER` | secret | none | Sent as the `authorization` header on export POSTs |

## The conventions the template encodes

- **The shared pieces come from `@proofoftech/flowsafe/host-kit`, not from
  here.** The whole Worker pipeline is `createFlowsafeWorker()` — the
  `/healthz` → approvals → runs → 404 fetch order, the auth seam
  (`parseActorTokens` + `bearerActorAuthenticator`), the run routes with
  their RBAC gate order (`createRunRouter`), the approval bridge
  (`queueApprovalForSuspension`, `resumeRunWithRequeue`), the two-cron
  `scheduled()` dispatch, and the audit-export `queue()` consumer — all
  security-critical and tested in the library. `worker.ts` supplies only what
  is deployment-specific: the workflows, the memoized `buildVerifier`, the
  cron expressions, and the optional subdomain cross-check (`wrapResolve`).
  Do not re-derive them.
- **Authenticate first, then construct.** The routers take a `TenantResolver`,
  not a bare `authenticate`: it verifies the token, validates the tenant claim,
  and binds the approval store to that tenant — so there is no pre-auth service
  for a later refactor to reach for. Swap `staticTokenVerifier` for
  `hmacVerifier` or your own `TokenVerifier` (JWKS, OIDC); everything else
  (role checks, separation of duties, self-approval denial, the tenant
  predicates) stays. With no `APPROVAL_ACTOR_TOKENS` secret the map is empty
  and every authenticated route 401s — fail closed.
- **The store factory is isolate-scoped, not request-scoped.**
  `D1ApprovalStoreFactory` owns one memoized schema-init pass; rebuilding it
  inside `fetch()` re-runs the whole DDL on every request. The template holds
  it in a module-scoped `WeakMap` keyed by the D1 binding.
- **A tenant is not a slug you can guess.** Provision through the `tenants`
  registry before issuing tokens, and read a tenant's *kind* from that registry
  rather than inferring it from its id.
- **The approval queue's create route is off.** `createApprovalRouter` mounts
  `POST /api/approvals` only when passed `allowCreate: true`, and even then a
  body may not set `connectors` (which *is* the minted grant), `requestedBy`
  (which separation-of-duties compares), or the fields selecting which leg a
  grant mints on. Approval records are authored in-process from an observed
  suspension. Never widen this.
- **A suspension is an approval request.** Both bridges (start and resume) run
  through `queueApprovalForSuspension()`, which captures the suspension's
  `(suspendedAt, resumeCount)` pair so grant minting binds the decision to that
  exact suspension (clock-free), and reads the suspend payload's `connectors`
  array so a decision mints exactly the grants the step asked for. That array
  must be a **server-authored static literal** — deriving it from run input
  would let client input choose its own capability. Multi-gate workflows
  re-enter the queue automatically on each re-suspension
  (`resumeRunWithRequeue`).
- **A workflow's metadata is its route contract.** Each entry in `WORKFLOWS`
  is served at `GET /workflows` and gates `POST /runs` via its optional
  `allowedRoles` (a subset of the coarse `RUN_START_ROLES` check that runs
  first). `defineWorkflows` asserts every listed id was actually committed.
- **Grants never travel in HTTP bodies.** The DO-side
  `approvalGrantProvider` derives `breakwater.approvedConnectors` from
  APPROVED records on every start/resume; the public raw-resume route stays
  grant-free and gated steps fail closed on forged resumes. A forged
  `resumeData.approved` can cosmetically flip a workflow boolean but grants
  no connector capability — the side-effecting step re-checks the
  server-derived grant, so treat `resumeData` as untrusted, never as the
  security boundary.
- **Separation of duties survives the bridge.** `queueApprovalForSuspension`
  attributes each auto-queued approval to the human who advanced the run
  (`requestedBy` = the starting actor, or the reviewer whose decision caused
  a re-suspension), not the `SYSTEM_ACTOR` bridge — so the library's
  self-decision check can fire and a start actor cannot approve their own
  run. Dropping `requestedBy` here silently disables that control.
- **Structured logs always, Queues export when bound.** Audit events, SLA
  escalations, config errors, and maintenance counts all go to Workers Logs
  as single-line JSON (`{"type":"audit"|"sla-escalation"|...}`). Uncomment
  the `queues` block in wrangler.jsonc and audit events additionally flow
  producer → queue → the `queue` consumer → an authenticated NDJSON POST to
  `SIEM_ENDPOINT` (`@proofoftech/flowsafe/audit-export`); a failed export
  retries the whole batch into Queues backoff and the dead-letter queue, so
  no event is acked before the collector confirmed it.

## Testing & verification

The template's correctness rests on four independently-maintained layers:

1. **Its own executable test.** `worker.e2e.test.ts` (part of `pnpm -r test`)
   drives the REAL exported handler in-process — real Mastra D1Store and
   `D1ApprovalStore` over `node:sqlite` behind a D1-shaped adapter, real
   `FlowsafeRunner` instances behind a stub DO namespace — and pins the auth
   seam (incl. the reserved-`system` token drop), the full
   start → auto-queued approval → SoD denials → reviewer decision →
   grant-minted publish loop, the fail-closed forged resume, the tenant
   boundary (client runId 400, cross-tenant 404s), `scheduled()` (SLA
   escalation + terminal-only retention purge, with the two surfaces isolated
   from each other's failures), and the `queue()` audit-export consumer.
2. **Typecheck.** `pnpm --filter @proofoftech/flowsafe typecheck` includes
   `deploy/tsconfig.json`, so the wiring is type-checked against flowsafe
   source through the real `@proofoftech/flowsafe/*` specifiers.
3. **The library test suite.** Every enforcement rule the template relies on
   — role authorization, CAS transitions, SLA/escalation, self-decision
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
