# Deploy Flowsafe on Cloudflare

This reference Worker connects the Flowsafe Durable Object runner and approval queue on Cloudflare. It uses one Durable Object per run, D1 for snapshots and approvals, bearer-token authentication, and a fixed alarm-driven maintenance singleton. Copy this directory into your project as the baseline for one organization.

Flowsafe's data plane is physically isolated. Each organization needs its own Worker, D1 database, and Durable Object namespaces. Before provisioning, replace every `replace-me` segment in `wrangler.jsonc` with the deployment tag; for example, tag `acme` uses Worker `anchorage-flowsafe-acme` and D1 database `anchorage-flowsafe-acme`. The Worker script name is also the Durable Object namespace boundary. Do not point multiple organizations at this deployment, reuse a script name, or add organization claims to bearer tokens.

The sibling [`../spike/worker.ts`](../spike/worker.ts) is the smaller deterministic workerd proof. This template adds production-shaped authentication, maintenance, multi-gate approval bridging, `/healthz`, optional live streaming, and audit export.

## Establish deployment identity

Provisioning stamps the same stable tag in two places:

- the `DEPLOYMENT_TENANT` variable in `wrangler.jsonc`;
- the singleton `flowsafe_deployment.tenant_tag` row in D1.

The tag must match `^[a-z0-9]{3,32}$`. It is infrastructure attribution, not a request claim. The Worker checks the pair before every fetch and maintenance duty. Missing, malformed, or mismatched identity fails closed; fetch returns `503`.

Production Durable Objects also require a `DEPLOYMENT_IDENTITY_SECRET`. Worker topologies stamp this internal credential on every Worker-to-object request, and each object compares it in constant time before reading storage. This prevents an accidental or hostile cross-script Durable Object binding from reaching another deployment even when that target's environment and D1 sentinel agree. Alarms have no caller request, so they validate the target environment and sentinel only.

Seed a new D1 database before application migrations or traffic:

```bash
pnpm --dir packages/flowsafe provision:deployment -- \
  --database anchorage-flowsafe-acme \
  --tag acme \
  --remote \
  --config deploy/wrangler.jsonc

pnpm --dir packages/flowsafe exec wrangler secret put \
  DEPLOYMENT_IDENTITY_SECRET \
  --config deploy/wrangler.jsonc
pnpm --dir packages/flowsafe exec wrangler secret put \
  MAINTENANCE_ADMIN_SECRET \
  --config deploy/wrangler.jsonc
```

These commands run from the Anchorage repository checkout. Applications that install `@proofoftech/flowsafe` can run the published binary as `pnpm exec flowsafe-provision` or `npx flowsafe-provision`; it uses the application's Wrangler 4 installation.

Provisioning verifies the exact sentinel schema and singleton row. It is idempotent for the same tag, refuses to re-stamp a database owned by another deployment, and refuses to adopt an unowned database that already contains any application table. Do not expose provisioning as a public route or overwrite a mismatched sentinel. Recreate the resource set instead. Library hosts can call `seedDeploymentIdentity(db, tag)` from an equivalent trusted provisioning process.

Run and thread ids are server-minted opaque values. Resource ids are validated host-owned business keys; never accept a full resource id from an untrusted request. Do not parse an organization from any id.

## Deploy the Worker

Run these commands from `packages/flowsafe`:

```bash
# 1. Replace every `replace-me` segment with `acme`, create the deployment's
#    uniquely named database, and paste the printed id into wrangler.jsonc.
pnpm exec wrangler d1 create anchorage-flowsafe-acme \
  --config deploy/wrangler.jsonc

# 2. Set DEPLOYMENT_TENANT in deploy/wrangler.jsonc, seed the same tag before
#    application migrations, and set both per-deployment internal credentials.
pnpm provision:deployment -- \
  --database anchorage-flowsafe-acme \
  --tag acme \
  --remote \
  --config deploy/wrangler.jsonc
pnpm exec wrangler secret put DEPLOYMENT_IDENTITY_SECRET \
  --config deploy/wrangler.jsonc
pnpm exec wrangler secret put MAINTENANCE_ADMIN_SECRET \
  --config deploy/wrangler.jsonc

# 3. Store a static actor map for the reference verifier. Production hosts
#    should replace this verifier with JWT, OIDC, or mTLS validation.
pnpm exec wrangler secret put APPROVAL_ACTOR_TOKENS \
  --config deploy/wrangler.jsonc
# Example: {"tok-ray":{"id":"ray","role":"reviewer"},
#           "tok-op":{"id":"op","role":"operator"}}

# 4. Optionally enable live WebSocket streams with a dedicated signing key.
pnpm exec wrangler secret put STREAM_TICKET_SECRET \
  --config deploy/wrangler.jsonc

# 5. Deploy after the sentinel is present, then bootstrap the maintenance alarm.
pnpm deploy:cf
curl -fsS -X POST https://your-worker.example/admin/ensure-maintenance \
  -H "authorization: Bearer ${maintenance_admin_secret}"
```

For local iteration, set `DEPLOYMENT_TENANT` to `acme`, seed the local database, then run `pnpm deploy:dev`:

```bash
pnpm provision:deployment -- \
  --database anchorage-flowsafe-acme \
  --tag acme \
  --local \
  --config deploy/wrangler.jsonc \
  --persist-to .wrangler/state
pnpm deploy:dev
```

Wrangler uses local D1 and Durable Object state, so local development does not require a Cloudflare account. Put distinct development-only `DEPLOYMENT_IDENTITY_SECRET` and `MAINTENANCE_ADMIN_SECRET` values of at least 32 visible ASCII characters in `deploy/.dev.vars`.

Do not upgrade a legacy pooled or differently owned script in place. Deploy the new tag-suffixed Worker name with a fresh D1 database and fresh Durable Object namespaces, verify it without public traffic, and then move the route. Reusing the old script name retains its Durable Object namespaces even when D1 is replaced.

Verify the approval path after deployment:

```bash
worker_url=https://your-worker.example
operator_token=tok-op
reviewer_token=tok-ray
approval_id=replace-with-approval-id
run_id=replace-with-run-id

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

Before production, bind a scratch Worker to a database stamped with a different tag. Confirm `/healthz` and protected routes return `503`, no workflow runs, and the log contains `deployment-identity-error`.

## Use the HTTP routes

Every route except `GET /healthz` requires a valid actor credential. `/healthz` is unauthenticated but still verifies deployment identity. `POST /api/stream/ticket` requires a bearer token; WebSocket routes authenticate the resulting short-lived ticket.

| Method and route | Purpose |
| --- | --- |
| `GET /healthz` | Verify deployment identity and return liveness |
| `GET /workflows` | List registered workflows and the authenticated actor |
| `POST /runs` | Start a server-id-assigned run and queue each observed suspension |
| `GET /runs/:workflowId/:runId` | Read run status and reconcile a missing approval record |
| `POST /runs/:workflowId/:runId/resume` | Resume without connector grants; approval-gated connectors still fail closed |
| `GET /api/approvals` | List and filter the deployment approval queue |
| `GET /api/approvals/metrics` | Read deployment queue metrics |
| `GET /api/approvals/:id` | Read one approval |
| `POST /api/approvals/:id/claim` | Claim a pending or escalated approval |
| `POST /api/approvals/:id/decide` | Approve or reject one approval and attempt to resume its run |
| `POST /api/approvals/:id/delegate` | Assign an open approval to another reviewer |
| `POST /api/approvals/batch/decide` | Apply one decision to at most 100 unique approval ids |
| `POST /api/stream/ticket` | Mint a 60-second hub or run-stream ticket when streaming is enabled |
| `GET /api/stream/hub?ticket=stream_ticket` | Open the deployment approval and presence WebSocket |
| `GET /api/stream/run/:workflowId/:runId?ticket=stream_ticket` | Open the run-progress WebSocket |

The Worker does not mount `POST /api/approvals`. Approval records come from observed workflow suspensions, not request bodies. The `/api/stream/*` routes exist only when both the `HUB` binding and `STREAM_TICKET_SECRET` are present.

## Configure the deployment

| Name | Kind | Default | Meaning |
| --- | --- | --- | --- |
| `DEPLOYMENT_TENANT` | variable | none; requests fail `503` | Required stable tag that must match the D1 sentinel |
| `DEPLOYMENT_IDENTITY_SECRET` | secret | none; requests fail `503` | Required 32–256 visible ASCII character internal credential shared only by this deployment's Worker and Durable Objects |
| `MAINTENANCE_ADMIN_SECRET` | secret | none; admin routes return `503` | Required 32–256 visible ASCII control-plane credential for maintenance bootstrap and status |
| `APPROVAL_ACTOR_TOKENS` | secret | none; protected routes return `401` | JSON map of bearer token to `{ id, role }`. Unknown roles and empty actor ids are dropped |
| `APPROVAL_SLA_SECONDS` | variable | `14400` | SLA applied to new approvals |
| `APPROVAL_ALLOW_SELF_DECISION` | variable | unset | Separation of duties remains enabled. `true` exempts all deciders; a comma-separated role list exempts only those roles |
| `RUN_RETENTION_DAYS` | variable | `30` | Terminal snapshot age before alarm-driven purge; `0` makes every terminal run eligible |
| `APPROVAL_RETENTION_DAYS` | variable | `30` | Decided approval age before alarm-driven purge |
| `THREAD_RETENTION_DAYS` | variable | unset | Optional idle thread and message retention duty |
| `STREAM_TICKET_SECRET` | secret | unset | Dedicated HMAC key for 60-second WebSocket tickets |
| `AUDIT_QUEUE` | Queue producer binding | unbound | Queues audit events for the shared control-plane consumer |

Keep authentication, stream-ticket, model-provider, connector, webhook, and SIEM secrets distinct.

## Understand alarm-driven maintenance

The fixed `deployment-maintenance` Durable Object stores the next sweep and purge times. Each alarm persists its successor before running one due duty. If both duties are due, the object schedules an immediate follow-up alarm instead of sharing the invocation. A Workers CPU-limit termination cannot break the alarm chain or starve the other duty.

After deployment, authenticate `POST /admin/ensure-maintenance` with `MAINTENANCE_ADMIN_SECRET`. Read `GET /admin/maintenance-status` with the same credential and alert when `alarmAt` is null or the last successful duty is stale.

- `sweepSLA(store, ...)` scans the deployment approval store and escalates open requests past `slaDeadlineAt`. It is maintenance code, not an HTTP service method.
- `purgeExpiredWorkflowRuns()` deletes terminal snapshots in bounded batches. Suspended and running runs remain at every age.
- `purgeExpiredApprovals()` deletes approved and rejected records. Pending, claimed, and escalated requests remain.
- `purgeExpiredThreads()` is optional. It deletes an idle thread with its messages and leaves working-memory resources intact.

Build the retention `R2ArtifactStore` from the current invocation binding with `artifactStore: (env) => new R2ArtifactStore(env.ARTIFACTS)`, using the same bucket as runtime writes. Snapshot rows are the enumerable record of artifact keys, so artifacts delete before the corresponding row. Factory or deletion failure keeps the row for retry.

If storage uses `tablePrefix`, configure the identical value as `storageTablePrefix` on `createFlowsafeWorker()`. The host accepts an empty prefix or a safe SQL identifier prefix that starts with an ASCII letter or underscore and continues with ASCII letters, numbers, or underscores. It applies the prefix to workflow-run, thread, background-task, notification, thread-state, and schedule-trigger purges. It does not auto-discover a prefix or apply it to fixed-schema Flowsafe tables.

Schedules, subscriptions, resources, and working memory have no TTL. The schedule and subscription routes delete their records explicitly. Resources and permanent thread teardown remain host-owned: remove every authoritative binding and wake source before releasing the corresponding ownership claims. Idle-thread retention deletes memory rows but deliberately keeps those claims. Deployment decommissioning removes whatever remains. Open approvals and live runs are never age-purged and remain until they reach a terminal state or the deployment is decommissioned. There is no in-database organization purge.

## Preserve the host conventions

- Reuse `createFlowsafeWorker()`, `createFlowsafeMaintenanceDurableObject()`, and the host-kit routers. They own deployment verification, route order, actor resolution, role gates, approval bridging, and maintenance dispatch.
- Replace only the `TokenVerifier` seam for production identity. A verified actor has `id` and `role`; it does not choose deployment identity.
- Keep `D1ApprovalStoreFactory` scoped to the isolate and call `.store()`. Rebuilding it in every request repeats schema initialization.
- Keep approval creation off the public API. The suspension bridge supplies server-authored connector ids, requester attribution, exact suspension identity, and resume targets.
- Treat every suspension as a new approval request. Preserve the `(suspendedAt, resumeCount)` pair and keep connector ids as server-authored static literals.
- Keep workflow metadata aligned with runtime registration. `assertWorkflowsRegistered()` verifies that the public catalog and committed definitions agree.
- Derive grants on the server with `approvalGrantProvider()`. Public resume data never confers connector authority.
- Preserve separation of duties across gates. Attribute queued approvals to the execution principal that advanced the run, not the maintenance principal.
- Use D1 idempotency and rate-limit stores when budgets must survive isolate replacement. In this single-organization data plane, connector keys and budgets are deployment-wide unless the application deliberately supplies another non-tenant logical scope outside Flowsafe.
- Emit structured audit and maintenance events. Queue export failures retry and dead-letter without acknowledging unconfirmed events.

## Test and verify the deployment

Run:

```bash
pnpm --filter @proofoftech/flowsafe lint
pnpm --filter @proofoftech/flowsafe typecheck
pnpm --filter @proofoftech/flowsafe test
pnpm --filter @proofoftech/flowsafe build
pnpm --filter @proofoftech/flowsafe spike:verify
```

`worker.e2e.test.ts` exercises the handler with non-fidelity SQLite adapters for route composition. The Wrangler harness covers deployment identity, maintenance bootstrap, durable alarm state, one-duty dispatch, and failure recovery against real D1 and workerd.

`spike:verify` adds the workerd boundary: suspend, kill, restart on persisted state, resume from an approved record, and refuse a mismatched deployment sentinel.

See the repository [deployment reference](../../../docs/deployment-reference.md), [operations runbook](../../../docs/operations-runbook.md), and [security threat model](../../../docs/security-threat-model.md) before exposing traffic.
