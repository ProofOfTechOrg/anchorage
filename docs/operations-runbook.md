# Operations Runbook

Procedures for operating an Anchorage deployment on Mastra plus Cloudflare Workers.

## Operating Roles

- Workflow owner: accountable for business outcome and acceptance criteria
- Builder: designs workflow, prompts, policies, and connectors using Mastra SDK
- Operator: manages deployments, monitors runs, handles incidents
- Reviewer: approves or rejects workflow tasks via flowsafe dashboard

## Build

```
pnpm install
pnpm build
pnpm test
```

## Deploy

### Mastra on Cloudflare Workers

Configure `CloudflareDeployer` (`@mastra/deployer-cloudflare`) on the Mastra instance, build, then deploy the generated output with wrangler:

```
npx mastra build
npx wrangler deploy
```

`mastra deploy` is a real CLI command but targets Mastra's hosted platform, not Cloudflare.

### flowsafe (DO runner + approval queue)

Spike-verified on workerd. A copy-ready reference
deployment lives in `packages/flowsafe/deploy/` — a production-shaped Worker
wiring one Durable Object per run, the D1-backed approval queue, bearer-token
auth, and a cron trigger owning the SLA sweep and snapshot retention purge.
Deploy checklist and configuration table: `packages/flowsafe/deploy/README.md`.

```
# from packages/flowsafe/
pnpm deploy:cf     # wrangler deploy --config deploy/wrangler.jsonc
pnpm deploy:dev    # local workerd, no Cloudflare account
```

Audit export to a SIEM is optional (`@proofoftech/flowsafe/audit-export` via a
Cloudflare Queue binding) and workflow artifacts persist to R2
(`@proofoftech/flowsafe/artifacts`).

### breakwater Middleware

Shipped as npm package `@proofoftech/breakwater` -- consumers add as a dependency.

## Run (Local Development)

```
pnpm dev
```

## Incident Response

| Symptom | Likely Cause | Action |
|---|---|---|
| Workflow stuck in "running" | DO CPU limit hit | Check Workers CPU limit logs; restart Durable Object |
| Approval not delivered | Cloudflare Queues backpressure | Check queue depth and consumer health |
| Suspended workflow cannot resume | Corrupted or orphaned run snapshot | Inspect the run's D1 snapshot; restore a consistent state |
| RBAC permission denied | Role missing or scope mismatch | Check audit log for `action: denied` entries |

## Tuning

- Durable Object alarm interval: lower for latency-sensitive approvals, higher for cost savings
- Approval SLA defaults: 4 business hours, escalate at 8 hours
- flowsafe dashboard polling: 10 second default, 30 seconds for viewer role
