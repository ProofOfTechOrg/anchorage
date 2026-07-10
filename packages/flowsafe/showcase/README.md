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
cp showcase/.dev.vars.example showcase/.dev.vars   # REQUIRED — the demo bearer tokens
pnpm --filter @proofoftech/flowsafe build         # bundle breakwater from dist
pnpm --filter @proofoftech/flowsafe app:build     # SPA → ../app/dist (served as assets)
pnpm --filter @proofoftech/flowsafe showcase:dev  # wrangler dev on :8787 — SPA at / + API same origin
```

The `.dev.vars` copy is not optional: `APPROVAL_ACTOR_TOKENS` is a **secret**,
not a wrangler var, so without it every authenticated route 401s. (`app:dev`
needs no copy — the in-process host reads the tokens from `demo-actors.ts`.)

Drive the loop with curl:

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

Local dev tokens (roles): `demo-admin`, `demo-builder`, `demo-operator`,
`demo-reviewer`, `demo-viewer` — all in the shared tenant `demo`. They live in
`showcase/demo-actors.ts`, the one source the dev switcher, the `app:dev` host,
and `.dev.vars.example` all derive from (`demo-actors.test.ts` fails if they
drift). They exist **only in dev**: the production SPA bundle contains no token
literal, and `scripts/assert-clean-app-bundle.mjs` fails the build if one
appears.

Note the queue's create route is **off**: `POST /api/approvals` returns 404.
Approval records are minted in-process from an observed suspension, never from a
request body — a body can carry neither `connectors` (which *is* the grant) nor
`requestedBy` (which is what separation-of-duties compares).

## The public demo (OAuth sandboxes)

Set `DEMO_JWT_SECRET` (secret) plus an OAuth client — `GOOGLE_CLIENT_ID` (var)
and `GOOGLE_CLIENT_SECRET` (secret), or the GitHub pair — and the showcase
grows a public sign-in. A provider counts only with its **full id+secret
pair**: a half-set pair logs a `config-error` and stays unmounted (an id
alone would advertise a sign-in that dies at the token exchange — and mask a
working fallback). One provider mounts per deployment; when both pairs are
configured, **Google wins** (the launch provider). The SPA reads the provider
name from `/auth/config`, so no client change is needed to switch. Subjects
are provider-scoped (`google:<sub>` / `github:<id>`), so switching providers
mints fresh sandboxes rather than colliding identities.

`GET /auth/<provider>` → OAuth → an **ephemeral tenant** provisioned through the
`tenants` registry, plus a set of short-TTL HS256 JWTs, one per demo role, all
bound to that tenant and each carrying a distinct `actor.id` (a shared id would
trip the self-approval check and no approval could ever complete). The token set
comes back in the URL **fragment**, so it never reaches a server log, and the
SPA scrubs it from history on read.

Everything a visitor runs and approves is invisible to every other visitor —
the same three invariants the commercial platform uses, not a demo-specific
shortcut.

Abuse controls, honestly:

- `UNIQUE(provider, subject)` gives one live sandbox per identity. That stops
  one account holding two; it does not stop N free accounts.
- Per-sandbox run cap **and** a global daily run ceiling, each enforced as a
  single conditional `UPDATE` (a `SELECT`-then-`UPDATE` is a TOCTOU race a
  burst of parallel starts walks straight through).
- `DEMO_DISABLED=true` is the kill switch, checked in the **auth middleware**,
  so already-issued JWTs stop verifying — not just new mints.
- Sandboxes expire after `DEMO_TENANT_TTL_HOURS`; a cron reaps them with
  `purgeTenant`, waiting out the JWT lifetime first so it never deletes runs
  out from under a still-valid token.
- Connectors stay binding-gated: a published demo cannot send an email, write
  a CRM, or deploy anything.

The real backstops react *after* spend. Size `DEMO_DAILY_RUN_CAP` for what you
can tolerate and set a billing alert.

## Single deploy

`showcase/wrangler.jsonc` has an `assets` block serving `../app/dist` at `/`
(`not_found_handling: single-page-application`), with `run_worker_first` keeping
the API routes (`/api/*`, `/runs`, `/runs/*`, `/healthz`, `/workflows`,
`/auth/*`) on the Worker — one origin, no build-time API URL. Two cron
expressions are declared and dispatched on `controller.cron`, so the SLA sweep
and the purge never share an invocation (a CPU-limit termination kills the
isolate and cannot be caught, so a slow sweep would starve the purge forever).

```bash
pnpm --filter @proofoftech/flowsafe showcase:deploy   # builds, then wrangler deploy
```

The deploy binds ONE public origin — `anchorage.proofoftech.org` (a Workers
custom domain; the zone must live on the deploying Cloudflare account) with
`workers_dev: false`, because the OAuth callback is registered for exactly
that origin. The SPA ships a TEMPORARY `noindex` robots meta
(`app/index.html`) until the demo is ready to be indexed.

A fresh deploy **401s on every authenticated route until you set the auth
secret** — no credentials are baked into `wrangler.jsonc`, so there is no state
in which the service is reachable with a token an attacker can read off GitHub:

```bash
wrangler d1 create anchorage-showcase              # paste the id into wrangler.jsonc
wrangler secret put APPROVAL_ACTOR_TOKENS \
  --config showcase/wrangler.jsonc                 # then flip connector bindings to go live
```

> **Do not paste the demo tokens in as that secret.** They are checked into this
> repository, so seeding them publishes world-known credentials — one of which is
> `admin`, who can both file and decide approvals. Generate real random tokens
> (or replace the bearer seam with your SSO/JWT verification in
> `bearerActorAuthenticator`).

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
