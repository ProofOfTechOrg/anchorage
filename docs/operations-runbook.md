# Operations runbook

This runbook covers a self-hosted Anchorage deployment on Cloudflare. Review the [Deployment reference](deployment-reference.md) and [Security threat model](security-threat-model.md) before production.

## Operating roles

Define people and escalation paths for:

- application owner: business outcome and policy;
- platform operator: Worker, D1, Durable Objects, R2, Queue, secrets, and incidents;
- connector owner: manifest accuracy, vendor behavior, credentials, and idempotency;
- reviewer: approval queue and separation of duties;
- security responder: identity, tenant isolation, audit, and vulnerability response.

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
pnpm --filter @proofoftech/breakwater test:packed-consumer
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

1. Confirm D1 binding and all append-only Durable Object migrations.
2. Confirm every enabled route has its required namespace or storage domain.
3. Confirm cron expressions exactly match the Worker constants.
4. Confirm the identity verifier fails closed with missing configuration.
5. Confirm tenant, stream-ticket, OAuth, model, webhook, connector, and SIEM secrets are distinct.
6. Confirm R2 artifact store is passed to runtime, retention, and offboarding.
7. Confirm audit Queue dead-letter policy and collector authorization.
8. Confirm a rollback bundle remains compatible with the deployed database and migration tags.

## Provision a tenant

Tenant ids match `^[a-z0-9]{3,32}$`. They are permanent identifiers, not display names.

```typescript
import { provisionTenant } from '@proofoftech/flowsafe/host-kit';

await provisionTenant(db, {
  tenantId: 'acme',
  kind: 'commercial',
});
```

Provision before issuing credentials. Insert-or-fail prevents two customers from sharing a slug. Reserved infrastructure ids and `system` cannot be allocated; `default` is reserved for a conventional single-tenant host.

Then:

1. Issue verified credentials carrying the exact tenant id.
2. Start a smoke run.
3. Confirm the run id begins with `acme_`.
4. Query it as the tenant.
5. Query it with a second tenant and confirm 404.
6. Repeat the ownership check for a thread if agents are enabled.

Do not reuse a purged tenant id.

## Offboard a tenant

1. Stop issuing credentials and scheduled work for the tenant.
2. Revoke all active credentials.
3. Wait for short-lived credentials and stream tickets to expire.
4. Disable or delete provider subscriptions and external webhooks if the vendor requires it.
5. Record the offboarding authorization.
6. Call `purgeTenant(db, { tenantId, artifactStore })`.
7. Verify every returned domain count and R2 deletion.
8. Confirm run, approval, thread, schedule, task, subscription, notification, goal, memory, and artifact access returns no tenant data.
9. Retain the tenant registry tombstone.

The purge deletes suspended and running snapshots too. It is the only cleanup for a run abandoned at a gate. Do not call it while credentials can still start or resume work.

## Scheduled duties

Use separate cron invocations for:

- approval SLA sweep;
- retention purge;
- schedule fire tick, when schedules are enabled.

A Workers CPU-limit termination kills the isolate and bypasses JavaScript `catch`. Separate invocations stop one runaway class from permanently starving another.

The purge invocation contains independent failure boundaries for each configured domain:

- terminal workflow snapshots and their artifacts;
- approved/rejected approvals;
- idle threads and their messages;
- terminal notifications;
- thread state and goals;
- schedule trigger history;
- terminal background tasks;
- deployment-owned extra data.

Provider polling runs through per-tenant Durable Object alarms. Subscription mutations should call the provider-host topology's `reconcilePolling()` after committing D1. A reconciliation failure means the mutation is already durable; retry reconciliation rather than trying to undo the row.

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
| Schedules, resources, subscriptions | Never by TTL; offboarding only |

Open approval requests and suspended/running runs remain at any age.

Set TTLs longer than the operational recovery window. A terminal approval can disappear before a delayed resume redrive; the next reconciliation may need a new human decision.

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
6. forge a raw resume on a fresh protected gate and confirm connector denial;
7. disconnect the live socket and confirm polling catches up;
8. if enabled, send a signal, fire a schedule, run a task, and reconcile a provider subscription.

## Audit and alerting

At minimum, alert on:

- authentication/configuration failures;
- connector denied or error decisions;
- approval SLA escalation;
- approval resume failures;
- audit sink and Queue retry/dead-letter activity;
- unknown cron expressions;
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
| Every protected route returns 401 | Identity secret, verifier parsing, tenant claim | Restore valid verifier configuration; do not add an unauthenticated fallback |
| One tenant receives 403 during resolution | Tenant charset or subdomain cross-check | Fix identity mapping or host routing |
| A foreign resource returns 403 instead of 404 | Route ownership ordering | Treat as an information-oracle regression and fix the shared boundary |
| Start returns duplicate-run conflict | Client retried after a response loss | Query the server-minted run id; do not mint a replacement blindly |
| Run stays `suspended` after approval | Decision result and resume outcome | Read stored decision, status, and audit; redrive through trusted resume |
| Durable agent resume fails after eviction | Prepare/observe registration and memory binding | Use `resumeViaRuntime()` through the thread topology; never raw inherited resume |
| Connector says approval missing after an approved record | Fingerprint, connector id, tenant-bound store | Confirm the record matches current step, `suspendedAt`, `resumeCount`, and exact connector id |
| Connector repeats an external write | Idempotency key, shared store, pending TTL | Fix the business key/store reach/TTL; inspect vendor idempotency evidence |
| One tenant throttles another | Missing isolation scope or in-memory rate store | Register `tenantIsolation()` and use D1 rate storage |
| Egress policy allowed an unexpected socket | Connector bypassed runtime fetch | Route the transport through guarded fetch and add infrastructure egress control |
| Live updates stop but HTTP works | Hub binding, ticket secret/expiry, socket liveness | Let client poll; repair stream configuration without disabling authorization |
| Subscription changed but polling did not | Reconcile response/audit and provider alarm | Retry `reconcilePolling()` against committed subscriptions |
| Schedule did not fire | Tick cron, active status, CAS winner, run cap, tenant metadata | Correct configuration or stored row; do not bypass the tick guard |
| Purge removed a snapshot but left R2 | Artifact store omitted or delete failed | Restore row/key evidence if available, repair paired purge, scan known prefix |
| Audit is absent while requests succeed | Sink, Queue, consumer, SIEM | Restore export, preserve local ring/Logs, assess evidence gap |
| Agent CLI error lacks output | Expected safe diagnostics | Inspect the isolated workspace/vendor logs under appropriate access; do not weaken public error safety |

## Recovery rules

- Do not edit a D1 snapshot manually unless you have a tested, version-specific repair and no supported redrive.
- Do not copy an approval grant into a resume request.
- Do not roll a terminal approval back to open.
- Do not delete a pending idempotency record while its owner may still run.
- Do not change a deployed Durable Object migration tag.
- Do not loosen the tenant charset to recover an invalid tenant.
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
- public-demo per-tenant and global daily run budgets.

Do not shorten a safety timeout below the maximum real operation merely to increase throughput. Scale execution and stores instead.
