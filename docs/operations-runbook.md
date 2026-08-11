# Operations runbook

This runbook covers a self-hosted Anchorage deployment on Cloudflare. Review the [Deployment reference](deployment-reference.md) and [Security threat model](security-threat-model.md) before production.

## Operating roles

Define people and escalation paths for:

- application owner: business outcome and policy;
- platform operator: Worker, D1, Durable Objects, R2, Queue, secrets, and incidents;
- connector owner: manifest accuracy, vendor behavior, credentials, and idempotency;
- reviewer: approval queue and separation of duties;
- security responder: identity, provisioning isolation, audit, and vulnerability response.

These operating roles are not the same as breakwater's five application role labels.

## Validate a release

From the repository:

```bash
corepack enable
pnpm install --frozen-lockfile
pnpm docs:check
pnpm docs:api
pnpm lint
pnpm typecheck
pnpm test
pnpm build
pnpm test:packed-breakwater
pnpm test:packed-flowsafe-agent-host
pnpm test:packed-flowsafe-provisioning
pnpm --filter @proofoftech/flowsafe test:signals-client-export
pnpm --filter @proofoftech/flowsafe typecheck:react18
pnpm --filter @proofoftech/flowsafe spike:verify
pnpm --filter anchorage-agent-starter test
pnpm --filter anchorage-agent-starter typecheck
```

The deterministic spike starts workerd, suspends runs, kills the process, restarts on the same D1 state, and proves resume plus fail-closed security cases. Do not replace it with an in-process-only test.

Run the optional live-model proof before changing model providers or durable-agent integration:

```bash
SPIKE_LLM_MODEL_ID='provider/model' \
SPIKE_LLM_API_KEY='secret' \
pnpm --filter @proofoftech/flowsafe spike:verify:llm
```

## Deploy

The baseline Worker is in [`packages/flowsafe/deploy/`](https://github.com/ProofOfTechOrg/anchorage/tree/main/packages/flowsafe/deploy). The advanced agent host is in [`packages/agent-starter/`](https://github.com/ProofOfTechOrg/anchorage/tree/main/packages/agent-starter).

Local baseline:

```bash
pnpm --filter @proofoftech/flowsafe deploy:dev
```

Production baseline:

```bash
pnpm --filter @proofoftech/flowsafe deploy:cf
```

Before deployment:

1. Confirm the Worker script name and D1 database name are unique to the deployment tag, and confirm all append-only Durable Object migrations. Never reuse a legacy pooled script name: it retains the old Durable Object namespaces.
2. Confirm `DEPLOYMENT_TENANT` matches the singleton sentinel in every bound D1 database.
3. Confirm `DEPLOYMENT_IDENTITY_SECRET` is distinct per deployment and reaches every Worker-to-Durable-Object topology.
4. Confirm every enabled route has its required namespace or storage domain.
5. Confirm the `MAINTENANCE` singleton binding and append-only migration exist, then authenticate `POST /admin/ensure-maintenance`.
6. Confirm the identity verifier fails closed with missing configuration.
7. Confirm internal-object, maintenance-admin, stream-ticket, OAuth, model, webhook, connector, and SIEM secrets are distinct.
8. Confirm R2 artifact storage is passed to runtime, retention, and the deployment decommission inventory.
9. Confirm audit Queue dead-letter policy and collector authorization.
10. Confirm a rollback bundle remains compatible with the deployed database and migration tags.

Before the first guarded agent-host deployment, drain approvals created by the legacy raw-agent target. Approve or reject them through the existing API so the state machine and audit record every transition, then verify that no open records remain:

```bash
pnpm --dir packages/agent-starter exec wrangler d1 execute anchorage-agent-starter-acme \
  --remote \
  --config wrangler.jsonc \
  --command "SELECT COUNT(*) AS open_agent_approvals FROM flowsafe_approvals WHERE workflow_id = 'durable-agentic-loop' AND status IN ('pending', 'claimed', 'escalated');"
```

Deploy only when `open_agent_approvals` is zero. Do not close these records through direct SQL mutation.

## Provision a deployment

Choose one stable deployment tag per organization. It must match `^[a-z0-9]{3,32}$` and is provisioning material, not a display name or request claim.

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

Run the provisioning command immediately after D1 creation and before application migrations. It verifies the strict singleton schema, is idempotent for the same tag, refuses to re-stamp a database owned by another deployment, and refuses an unowned database with application tables. Do not expose it as a public route. Generate distinct 32–256 visible ASCII deployment-identity and maintenance-admin credentials for each deployment.

Then:

1. Allocate a dedicated tag-suffixed Worker script name, D1 database, and set of Durable Object namespaces. Never deploy over a retained script from another organization or a legacy pooled deployment. Allocate dedicated R2, Queue, and secret resources where those domains are enabled.
2. Seed the new D1 database, then apply application migrations.
3. Set `DEPLOYMENT_TENANT` to the seeded tag and bind the internal credential only to that deployment's Worker and Durable Objects.
4. Record the one-to-one organization-to-resource mapping in the control plane.
5. Deploy without public traffic, authenticate `POST /admin/ensure-maintenance`, and confirm `GET /admin/maintenance-status` has a non-null `alarmAt`.
6. Call a protected route and confirm the sentinel check succeeds before issuing user credentials.
7. Issue verified credentials that carry only the actor id, role, and any application-specific identity claims.
8. Start a smoke run and confirm the host returns opaque run and thread ids without an organization prefix.
8. In a scratch deployment, bind the Worker to a database with a different sentinel and confirm protected routes return `503` without reading application data.
9. Bind a scratch Worker namespace to another deployment's Durable Objects and confirm the wrong internal credential is rejected before object storage is read.
10. Restore the correct bindings and repeat the smoke run.

Never bind two organizations to one data-plane resource set. Never reuse a tag while retained resources or audit records could make its ownership ambiguous.

## Decommission a deployment

1. Stop new traffic, scheduled work, and credential issuance for the deployment.
2. Revoke active credentials and wait for short-lived credentials and stream tickets to expire.
3. Disable provider subscriptions and external webhooks when the vendor requires explicit cleanup.
4. Record the decommissioning authorization and immutable resource inventory.
5. Export records that policy requires you to retain, including audit evidence.
6. Remove routes and delete the Worker so no code can open the bound stores.
7. Delete the deployment's D1 databases, Durable Object namespaces and storage, R2 buckets, Queue bindings or dedicated queues, and secrets.
8. Verify the former hostname and every recorded resource identifier are absent or inaccessible.
9. Retain a control-plane tombstone that records the tag, authorization, completion time, and deletion results.

There is no in-database organization purge. Abandoned live runs and approvals disappear only when the physical database is deleted, so revoke traffic before deletion and treat the resource inventory as the completion checklist.

## Alarm-driven duties

The maintenance singleton runs separate alarm invocations for:

- approval SLA sweep;
- retention purge;
- schedule fire tick, when schedules are enabled.

A Workers CPU-limit termination kills the isolate and bypasses JavaScript `catch`. The object persists the next alarm before each duty and runs one duty per invocation, so one runaway class cannot permanently starve another.

The purge invocation contains independent failure boundaries for each configured domain:

- terminal workflow snapshots and their artifacts;
- approved/rejected approvals;
- idle threads and their messages;
- terminal notifications;
- thread state and goals;
- schedule trigger history;
- terminal background tasks;
- deployment-owned extra data.

Provider polling runs through the singleton provider-host Durable Object's alarms. Subscription mutations should call the provider-host topology's `reconcilePolling()` after committing D1. A reconciliation failure means the mutation is already durable; retry reconciliation rather than trying to undo the row.

## Retention policy

| Data | Eligible |
| --- | --- |
| Workflow snapshots | Terminal statuses older than run TTL |
| Approval records | Approved or rejected older than approval TTL |
| Threads and messages | Thread idle longer than optional thread TTL |
| Notifications | Terminal rows older than optional notification TTL |
| Thread state and goals | Rows older than optional state TTL |
| Schedule triggers | Fires older than optional trigger TTL |
| Background tasks | Terminal rows older than task TTL |
| Schedules and subscriptions | Never by TTL; authorized deletion or deployment decommissioning |
| Resources and working memory | Never by TTL; explicit host teardown or deployment decommissioning |
| `flowsafe_resource_owners` | Run retention and schedule deletion release their claims. Thread and resource claims require explicit host teardown or deployment decommissioning |

Open approval requests and suspended/running runs remain at any age.

Set TTLs longer than the operational recovery window. A terminal approval can disappear before a delayed resume redrive; the next reconciliation may need a new human decision. Idle-thread retention removes memory rows but keeps thread and resource ownership because standing wake sources can recreate the memory. TTL retention does not replace an authorized deletion workflow or the resource inventory used during deployment decommissioning.

## Health checks after deploy

```bash
curl https://worker.example/healthz

curl https://worker.example/workflows \
  -H 'authorization: Bearer operator-token'

curl https://worker.example/api/approvals \
  -H 'authorization: Bearer reviewer-token'
```

Exercise:

1. start a workflow;
2. observe an approval;
3. attempt self-decision and expect the configured result;
4. approve as an independent reviewer;
5. observe run completion;
6. start a guarded agent and observe its suspension;
7. confirm every public agent resume path returns 404;
8. restart the Worker, approve as a different reviewer, and confirm the original requester remains the execution principal;
9. disconnect the live socket and confirm polling catches up;
10. evict agent stream replay and confirm status remains authoritative after the stream returns 409;
11. if enabled, send a signal, fire a schedule, run a task, and reconcile a provider subscription.

## Audit and alerting

At minimum, alert on:

- authentication/configuration failures;
- deployment sentinel or internal Durable Object credential missing, malformed, or mismatched;
- control-plane drift from the one-to-one organization/resource mapping;
- connector denied or error decisions;
- approval SLA escalation;
- approval resume failures;
- audit sink and Queue retry/dead-letter activity;
- maintenance bootstrap, stale-alarm recovery, and duty failures;
- any purge duty failure;
- provider alarm/reconciliation failure;
- schedule skip due to run cap or invalid stored data;
- foreign run/thread probes;
- repeated signal or webhook rate-limit denial;
- idempotency store degradation after a successful side effect;
- Agent CLI timeout, spawn failure, or non-zero exit;
- demo global run budget near exhaustion.

Audit records are security evidence. Queue depth and SIEM ingestion status must be monitored separately from application health because sink failure is intentionally contained.

## Common incidents

| Symptom | Check | Action |
| --- | --- | --- |
| Every protected route returns 401 | Identity secret and verifier parsing | Restore valid verifier configuration; do not add an unauthenticated fallback |
| Protected routes return 503 before routing | `DEPLOYMENT_TENANT`, `DEPLOYMENT_IDENTITY_SECRET`, D1 sentinel, or a wrong database/object binding | Restore the verified one-to-one bindings and secret. Never overwrite the sentinel to adopt a database |
| A foreign resource returns 403 instead of 404 | Route ownership ordering | Treat as an information-oracle regression and fix the shared boundary |
| Start returns duplicate-run conflict | Client retried after a response loss | Query the server-minted run id; do not mint a replacement blindly |
| Run stays `suspended` after approval | Decision result and resume outcome | Read stored decision, status, and audit; redrive through trusted resume |
| Durable agent resume fails after eviction | Prepare/observe registration and memory binding | Use `resumeViaRuntime()` through the thread topology; never raw inherited resume |
| Agent stream returns 409 | In-memory replay cache was evicted or the isolate restarted | Read the authoritative status route; reconnect only for events still present in the configured cache |
| Connector says approval missing after an approved record | Fingerprint, connector id, workflow, run, and deployment store | Confirm the record matches current step, `suspendedAt`, `resumeCount`, and exact connector id |
| Connector repeats an external write | Idempotency key, shared store, pending TTL | Fix the business key/store reach/TTL; inspect vendor idempotency evidence |
| One workload throttles another | Deployment-wide connector budget | Tune the deployment limit or split workloads into separate deployments when they require independent capacity |
| Egress policy allowed an unexpected socket | Connector bypassed runtime fetch | Route the transport through guarded fetch and add infrastructure egress control |
| Live updates stop but HTTP works | Hub binding, ticket secret/expiry, socket liveness | Let client poll; repair stream configuration without disabling authorization |
| Subscription changed but polling did not | Reconcile response/audit and provider alarm | Retry `reconcilePolling()` against committed subscriptions |
| Schedule did not fire | Maintenance alarm status, active status, CAS winner, run cap, and stored metadata | Re-arm maintenance or correct the stored row; do not bypass the tick guard |
| Purge removed a snapshot but left R2 | Artifact store omitted or delete failed | Restore row/key evidence if available, repair paired purge, scan known prefix |
| Audit is absent while requests succeed | Sink, Queue, consumer, SIEM | Restore export, preserve local ring/Logs, assess evidence gap |
| Agent CLI error lacks output | Expected safe diagnostics | Inspect the isolated workspace/vendor logs under appropriate access; do not weaken public error safety |

## Recovery rules

- Do not edit a D1 snapshot manually unless you have a tested, version-specific repair and no supported redrive.
- Do not copy an approval grant into a resume request.
- Do not roll a terminal approval back to open.
- Do not delete a pending idempotency record while its owner may still run.
- Do not change a deployed Durable Object migration tag.
- Do not overwrite or bypass a deployment sentinel to recover a mis-bound database.
- Do not expose system stores or raw namespace access as an emergency endpoint.

Prefer replaying a trusted orchestration step from authoritative persisted state.

## Capacity tuning

Tune from measured workload:

- approval list page size and poll interval;
- SLA sweep frequency;
- purge batch sizes and TTLs;
- signal body and rate caps;
- schedule count, fire rate, and unattended-run cap;
- notification dispatch chunk size;
- provider poll interval and webhook rate cap;
- connector fixed-window count;
- idempotency pending TTL;
- Agent CLI timeout and retained output bytes;
- Queue batch and retry/dead-letter settings;
- public-demo per-session and global daily run budgets.

Do not shorten a safety timeout below the maximum real operation merely to increase throughput. Scale execution and stores instead.
