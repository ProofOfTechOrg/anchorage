# Anchorage Showcase — five workflows, one frontend, one deploy

The five `docs/examples/*` design sketches grown into **actually executing**
workflows on the flowsafe Durable Object runner, unified behind one React
frontend (a launcher + the approval dashboard + an actor switcher) and shipped as
a **single Cloudflare deploy** — one Worker serving both the API and the SPA.

| # | id | shape | what it showcases |
|---|----|-------|-------------------|
| 1 | `gtm-outbound` | serial + 1 gate | write-approval grant, binding-gated Cloudflare Email Service send, audit |
| 2 | `content-pipeline` | `.parallel()` → gate → publish | parallel fan-out/fan-in, R2 artifact write, content-hash idempotency |
| 3 | `lead-generation` | `.branch()` hot/cold → gate → assign | conditional branch, egress allowlist + rate-limit on the CRM write |
| 4 | `product-launch` | serial, **2 gates** | multi-checkpoint re-suspension, destructive + idempotent deploy, dry-run pre-flight |
| 5 | `access-request` | serial + gate, RBAC-scoped | route-level `allowedRoles`, cross-workflow isolation, separation of duties |

## Binding-gated: real spine, zero secrets offline

Every workflow's side effect is **binding-gated**. With no binding configured, a
connector renders/logs its envelope and reports a `simulated` outcome — no live
API call, no secrets — while STILL running the connector's **real `execute`**, so
the approval-grant gate is genuinely exercised: an unapproved (forged) resume
reaches the write step with no grant and **fails closed**. content-pipeline's R2
write is offline-real via an in-memory artifact bucket. Flip a binding (email,
CRM/deploy egress) to go live per connector — no code change.

## Run it — the full UI (recommended)

```bash
pnpm --filter @proofoftech/flowsafe app:dev   # Vite on :4321 + the in-process showcase host
```

Open http://localhost:4321. The **ActorSwitcher** picks a demo identity; the
**LauncherPanel** starts any of the five workflows (edit the sample JSON); the
**RunStatusPanel** polls each run to `success`; the **approval dashboard** below
is where you claim/decide. Try:

- Launch `gtm-outbound` as `operator` → it suspends → switch to `reviewer`,
  approve it in the queue → watch it reach `success`.
- Switch to `operator` and try to launch `access-request` → blocked (RBAC:
  admin/builder only). Switch to `builder` → it starts.
- Start a run as `admin`, then try to approve it as `admin` → denied
  (separation of duties). Approve as `reviewer` instead.
- `product-launch` clears **two** gates (approve gate 1 as `reviewer`, gate 2 as
  a different reviewer or `admin`).

## Run it — the deployed shape (workerd)

```bash
pnpm --filter @proofoftech/flowsafe build         # bundle breakwater from dist
pnpm --filter @proofoftech/flowsafe app:build     # SPA → ../app/dist (served as assets)
pnpm --filter @proofoftech/flowsafe showcase:dev  # wrangler dev on :8787 — SPA at / + API same origin
```

Drive the loop with curl (demo bearer tokens are baked as a wrangler var):

```bash
# start a run → suspends at its gate, auto-queues an approval
curl -sX POST localhost:8787/runs \
  -H 'authorization: Bearer demo-operator' -H 'content-type: application/json' \
  -d '{"workflowId":"content-pipeline","inputData":{"topic":"durable workflows"}}'

# a DIFFERENT reviewer approves → grant minted server-side, the run resumes
curl -sX POST localhost:8787/api/approvals/<id>/decide \
  -H 'authorization: Bearer demo-reviewer' -H 'content-type: application/json' \
  -d '{"decision":"approve"}'

# inspect: status "success" (content-pipeline writes the article to R2)
curl -s localhost:8787/runs/content-pipeline/<runId> -H 'authorization: Bearer demo-viewer'
```

Fail-closed proof (skip the queue, forge the resume → no grant, connector denied):

```bash
curl -sX POST localhost:8787/runs/content-pipeline/<runId>/resume \
  -H 'authorization: Bearer demo-operator' -H 'content-type: application/json' \
  -d '{"step":"reviewContent","resumeData":{"approved":true}}'
# → status "failed" (the connector's write gate denies: approval required, not granted)
```

Demo tokens (roles): `demo-admin`, `demo-builder`, `demo-operator`,
`demo-reviewer`, `demo-viewer`.

## Single deploy

`showcase/wrangler.jsonc` has an `assets` block serving `../app/dist` at `/`
(`not_found_handling: single-page-application`), with `run_worker_first` keeping
the API routes (`/api/*`, `/runs`, `/runs/*`, `/healthz`, `/workflows`) on the
Worker — one origin, no build-time API URL.

```bash
pnpm --filter @proofoftech/flowsafe showcase:deploy   # builds, then wrangler deploy
```

For a real deployment: `wrangler d1 create anchorage-showcase` (paste the id),
replace the demo `APPROVAL_ACTOR_TOKENS` var with a secret
(`wrangler secret put APPROVAL_ACTOR_TOKENS`), and flip each connector's binding
to go live.

## Going live per connector

| connector | offline | live |
| --------- | ------- | ---- |
| gtm-outbound `outreach-email` | logs envelope | onboard a domain, uncomment `send_email`, bind `EMAIL` |
| content-pipeline `publish-article` | in-memory bucket | bind an R2 bucket, pass it as `artifactBucket` |
| lead-generation `crm-assign` | logs assignment | pass `crm: { fetch, endpoint, token }` (host = the egress allowlist) |
| product-launch `release-deploy` | logs deploy | pass `deploy: { fetch, endpoint, token }` |

The safety spine (grant gate, approval queue, audit, RBAC, SoD, idempotency,
egress) is unchanged whether a connector runs live or simulated.

## Verify

```bash
pnpm --filter @proofoftech/flowsafe typecheck   # includes showcase/tsconfig.json
pnpm --filter @proofoftech/flowsafe test         # worker.e2e + workflows.e2e
```
