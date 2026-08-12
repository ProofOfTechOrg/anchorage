# Anchorage showcase: six workflows, seven guardrail scenarios, one deploy

The showcase runs six workflows on the flowsafe Durable Object runner and seven
deterministic guardrail scenarios through breakwater. One React application
combines the control room, workflow launcher, approval dashboard, run status,
and actor switcher. One Cloudflare Worker serves both the API and the SPA.

All six workflows are available from the launcher. `wire-transfer` also appears
in the control room, where it hands a real durable approval into the same review
queue.

| # | id | shape | what it showcases |
|---|----|-------|-------------------|
| 1 | `gtm-outbound` | serial + 1 gate | write-approval grant, binding-gated Cloudflare Email Service send, audit |
| 2 | `content-pipeline` | `.parallel()` → gate → publish | parallel fan-out/fan-in, R2 artifact write, content-hash idempotency |
| 3 | `lead-generation` | `.branch()` hot/cold → gate → assign | conditional branch, egress allowlist + rate-limit on the CRM write |
| 4 | `product-launch` | serial, **2 gates** | multi-checkpoint re-suspension, destructive + idempotent deploy, dry-run pre-flight |
| 5 | `access-request` | serial + gate, RBAC-scoped | route-level `allowedRoles`, cross-workflow isolation, separation of duties |
| 6 | `wire-transfer` | control-room agent + gate | prompt-injection defense, durable human approval, exact-suspension grant |

The seven in-browser scenarios exercise real Breakwater policies and evaluators: PII leakage, secret exfiltration, prompt injection, role enforcement, egress allowlists, cross-workflow isolation, and fail-closed opaque isolation scope. Their inputs are deterministic and require no model key. The adjacent wire-transfer card is the eighth control-room card and the only scenario that starts a server-side workflow.

## Binding-gated: real spine, zero secrets offline

Every workflow's side effect is **binding-gated**. With no binding configured, a
connector renders/logs its envelope and reports a `simulated` outcome — no live
API call, no secrets — while STILL running the connector's **real `execute`**, so
the approval-grant gate is genuinely exercised: an unapproved (forged) resume
reaches the write step with no grant and **fails closed**. content-pipeline's R2
write is offline-real via an in-memory artifact bucket. Flip a binding (email,
CRM/deploy egress) to go live per connector — no code change.

## Run it — the full UI (recommended)

Create `.dev.vars` and provision the local deployment sentinel using the commands in the workerd section below, then start the combined Vite and Worker development server:

```bash
pnpm dev        # Vite on :4321 + the real Cloudflare Worker and bindings
```

Open http://localhost:4321. The **ActorSwitcher** picks a demo identity; the
**LauncherPanel** starts any of the six workflows (edit the sample JSON); the
**RunStatusPanel** polls each run to `success`; the **approval dashboard** below
is where you claim/decide. Try:

- Launch `gtm-outbound` as `operator` → it suspends → switch to `reviewer`,
  approve it in the queue → watch it reach `success`.
- Switch to `operator` and try to launch `access-request` → blocked (RBAC:
  admin/builder only). Switch to `builder` → it starts.
- This demo relaxes separation of duties for `admin` only (via
  `APPROVAL_ALLOW_SELF_DECISION`), so `admin` can approve its own runs — that is
  how one operator drives everything. Every other decider stays bound.
- `product-launch` clears **two** gates. Drive it all as `admin`, or approve
  gate 1 as `reviewer` then gate 2 as `admin` — approving gate 2 as the *same*
  `reviewer` who decided gate 1 is denied (separation of duties: the gate-1
  decider becomes gate 2's requester).

## Run it — the deployed shape (workerd)

```bash
cp packages/showcase/.dev.vars.example packages/showcase/.dev.vars # REQUIRED — demo bearer tokens
pnpm --dir packages/flowsafe provision:deployment -- \
  --database anchorage-showcase-single-tenant \
  --tag showcase \
  --local \
  --config ../showcase/wrangler.jsonc \
  --persist-to ../showcase/.wrangler/state
pnpm --filter showcase build        # Worker + SPA → ./dist
pnpm --filter showcase preview      # built Worker on :8787 — SPA + API same origin
```

The `.dev.vars` copy is not optional. It supplies the local-only internal Durable Object credential and `APPROVAL_ACTOR_TOKENS`; without them requests fail closed. The provisioning command seeds the same local state directory that both `pnpm dev` and `preview` open.

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

Fail-closed proof: start a second run, copy its `runId`, then skip the queue and
forge the resume. The connector receives no grant and denies the write:

```bash
curl -sX POST localhost:8787/runs \
  -H 'authorization: Bearer demo-operator' -H 'content-type: application/json' \
  -d '{"workflowId":"content-pipeline","inputData":{"topic":"forged resume"}}'

curl -sX POST localhost:8787/runs/content-pipeline/<runId>/resume \
  -H 'authorization: Bearer demo-operator' -H 'content-type: application/json' \
  -d '{"step":"reviewContent","resumeData":{"approved":true}}'
# → status "failed" (the connector's write gate denies: approval required, not granted)
```

Local dev tokens (roles): `demo-admin`, `demo-builder`, `demo-operator`,
`demo-reviewer`, `demo-viewer`. They all act inside the shared demo organization. They live in
`worker/demo-actors.ts`, the one source the dev switcher, the `pnpm dev` host,
and `.dev.vars.example` all derive from (`demo-actors.test.ts` fails if they
drift). They exist **only in dev**: the production SPA bundle contains no token
literal, and `scripts/assert-clean-app-bundle.mjs` fails the build if one
appears.

Note the queue's create route is **off**: `POST /api/approvals` returns 404.
Approval records are minted in-process from an observed suspension, never from a
request body — a body can carry neither `connectors` (which *is* the grant) nor
`requestedBy` (which is what separation-of-duties compares).

## The public demo (one shared organization)

Set `DEMO_JWT_SECRET` plus one complete OAuth client pair: Google id and secret, or the GitHub pair. A half-configured provider logs a configuration error and stays unmounted. When both complete pairs exist, Google mounts. The SPA reads the selected provider from `/auth/config`.

`GET /auth/<provider>` completes OAuth and creates or replaces an expiring visitor session. It returns four short-TTL HS256 role tokens in the URL fragment, so tokens do not enter server logs and the SPA removes the fragment from browser history. Each role token has a distinct actor id for separation of duties.

Every visitor joins the same deployment-wide demo organization. The approval queue is intentionally shared so visitors can observe collaborative review. Run, thread, and resource ownership remains per principal: operators and builders receive `404` for another visitor's resources, while reviewers, viewers, and admins can read existing resources as their roles require. A visitor session controls identity, token expiry, and run allowance only; deleting an expired session does not delete workflow state.

Abuse controls:

- `UNIQUE(provider, subject)` allows one live session per provider identity.
- A per-session run cap and global daily ceiling each use one conditional D1 update, avoiding a select-then-update race.
- `DEMO_DISABLED=true` is checked during authentication, so it invalidates already-issued demo JWTs as well as disabling new sign-ins.
- Sessions expire after `DEMO_SESSION_TTL_HOURS`; retention removes session and budget rows only after their JWTs can no longer verify.
- There is no public reset route. One visitor must never erase the shared organization's records.
- Connectors stay binding-gated, so the published demo cannot send email, write a CRM, or deploy anything.

The real backstops react *after* spend. Size `DEMO_DAILY_RUN_CAP` for what you
can tolerate and set a billing alert.

## Blue/green deployment

`wrangler.jsonc` has an `assets` block whose client directory is generated by the Vite build (`not_found_handling: single-page-application`). `run_worker_first` keeps `/api/*`, `/admin/*`, `/runs`, `/runs/*`, `/healthz`, `/workflows`, and `/auth/*` on the Worker. The fixed maintenance Durable Object runs the SLA sweep and purge in separate alarm invocations, so a CPU-limit termination in one duty cannot starve the other.

The checked-in configuration is the live green isolation deployment: `anchorage-showcase-single-tenant` with `workers_dev: false`, preview URLs disabled, and `anchorage.proofoftech.org` as its custom domain. Its distinct script name owns dedicated Durable Object namespaces and a dedicated D1 database. The retained `anchorage-showcase` script, database, and namespaces are the legacy pre-isolation rollback bundle for the completed migration; they are not the rollback target for a later replacement cycle.

A fresh replacement **returns `401` from every authenticated route until you set its authentication secrets**. `wrangler.jsonc` contains no credentials, so repository access never grants an application credential. For the next cycle, the current live single-tenant deployment becomes the rollback bundle and the replacement becomes the new green candidate. The legacy pre-isolation bundle stays outside that cycle. Prepare the candidate with a separate script, database, and temporary Wrangler configuration:

```bash
next_worker=anchorage-showcase-next
next_database=anchorage-showcase-next
next_config="$PWD/packages/showcase/wrangler.next.jsonc"
pnpm exec wrangler d1 create "$next_database" \
  --config packages/showcase/wrangler.jsonc
cp packages/showcase/wrangler.jsonc "$next_config"
```

Edit only `wrangler.next.jsonc` for staging. Set `name` and the D1 database name to the two values above, and paste the new D1 id. Set `workers_dev` to `true`, keep `preview_urls` set to `false`, and set `routes` to an empty array. Do not change the checked-in live configuration or reuse its D1 id.

Seed the new database, set the four production secrets and a temporary staging
actor, then deploy the temporary configuration. Generate the actor token for
this replacement; never reuse the checked-in demo tokens.

Generate the maintenance administrator credential in the deployment secret
manager, configure the fleet control plane with that same value, and load it
into the shell as `maintenance_admin_secret`. The shell variable is only a
transient copy; it must not be the credential's system of record.

The Cloudflare Vite plugin writes `.wrangler/deploy/config.json`, and Wrangler
follows that generated redirect even when `--config` names a sibling source
file. Build from the replacement config before the first secret write, and
repeat that build after every later config edit. This keeps provisioning,
secret, and deploy commands pointed at the replacement Worker.

```bash
CLOUDFLARE_VITE_WRANGLER_CONFIG_PATH="$next_config" \
  pnpm --filter showcase build
: "${maintenance_admin_secret:?load the fleet maintenance credential from the deployment secret manager}"
pnpm --dir packages/flowsafe provision:deployment -- \
  --database "$next_database" \
  --tag showcase \
  --remote \
  --config "$next_config"
pnpm exec wrangler secret put DEPLOYMENT_IDENTITY_SECRET --config "$next_config"
printf '%s\n' "$maintenance_admin_secret" | \
  pnpm exec wrangler secret put MAINTENANCE_ADMIN_SECRET --config "$next_config"
pnpm exec wrangler secret put DEMO_JWT_SECRET --config "$next_config"
pnpm exec wrangler secret put GOOGLE_CLIENT_SECRET --config "$next_config"
staging_actor_token=$(openssl rand -hex 32)
printf '{"%s":{"id":"staging-operator","role":"admin"}}\n' \
  "$staging_actor_token" | \
  pnpm exec wrangler secret put APPROVAL_ACTOR_TOKENS --config "$next_config"
pnpm exec wrangler deploy --config "$next_config"
curl -fsS -X POST "https://${next_worker}.gcharang.workers.dev/admin/ensure-maintenance" \
  -H "authorization: Bearer ${maintenance_admin_secret}"
curl -fsS "https://${next_worker}.gcharang.workers.dev/workflows" \
  -H "authorization: Bearer ${staging_actor_token}"
curl -fsS -X POST "https://${next_worker}.gcharang.workers.dev/runs" \
  -H "authorization: Bearer ${staging_actor_token}" \
  -H "content-type: application/json" \
  -d '{"workflowId":"content-pipeline","inputData":{"topic":"replacement smoke test"}}'
```

Treat the cutover and rollback bundle as one resource set:

Cloudflare Workers Builds triggers belong to a Worker's immutable script tag;
copying `wrangler.jsonc` does not move them. Use the
[Workers Builds API](https://developers.cloudflare.com/workers/ci-cd/builds/api-reference/)
or the Cloudflare dashboard to transfer both connected triggers as part of the
cutover. Keep the production trigger on `main` with `pnpm -r build` followed by
`npx wrangler deploy`. Keep the preview trigger on non-`main` branches with the
same build followed by `npx wrangler versions upload --dry-run`. Both retain
the existing repository connection, build token, `/packages/showcase` root,
path filters, and cache setting.

1. Create the fresh D1 database and put its id only in the temporary configuration.
2. Seed the sentinel before any application table, then set the deployment identity and authentication secrets on the replacement script.
3. Deploy the temporary staging configuration. Use the generated staging actor for an authenticated route and a smoke run at its workers.dev URL; also verify `/healthz` and maintenance. OAuth sign-in is unavailable there because the provider callback remains the production origin.
4. Keep the current live `anchorage-showcase-single-tenant` script and its dedicated database and namespaces unchanged as the rollback bundle. Never bind either generation to the other's storage. Do not use the legacy `anchorage-showcase` bundle as the rollback target for this cycle.
5. Freeze unrelated repository pushes; only the controlled cutover release may advance until this procedure explicitly releases the freeze. In the temporary configuration, set `workers_dev` and `preview_urls` to `false`, then add `{ "pattern": "anchorage.proofoftech.org", "custom_domain": true }` to `routes`. Rebuild with `CLOUDFLARE_VITE_WRANGLER_CONFIG_PATH="$next_config"`, then deploy that configuration to move the custom domain to the replacement script.
6. Keep the push freeze held after verifying production OAuth and authenticated routes, then run `pnpm exec wrangler secret delete APPROVAL_ACTOR_TOKENS --config "$next_config"`. Snapshot both Workers Builds triggers on the previously live Worker, recreate them on the replacement with the exact settings above, and verify the replacement script tag, repository connection, commands, root, and filters by provider readback. Run a preview branch and require its replacement-Worker check to pass, then remove both triggers from the previously live Worker. Copy the temporary configuration over `wrangler.jsonc`, remove the temporary file, and commit the new source of truth through the release workflow as the only allowed push. Require the resulting `main` build to deploy a new version on the replacement Worker before releasing the push freeze and completing the cutover. The temporary operator credential must not remain on the public deployment.
7. To roll back, freeze unrelated repository pushes before restoring traffic and keep the freeze held through the complete rollback. Reverse the Workers Builds transfer: recreate both exact triggers on the retained previously live Worker, verify their immutable target by provider readback, and remove them from the replacement. Restore the custom domain from the retained revision of the previously live `anchorage-showcase-single-tenant` deployment and redeploy that exact Worker with its own storage. Do not make a rollback commit until the trigger target is correct; allow only that controlled commit, require its `main` build to deploy the retained Worker, then release the freeze. Do not attach the legacy pre-isolation Worker or mix any generation's storage.
8. After the rollback window closes, decommission the previously live single-tenant Worker, database, Durable Object namespaces, and associated secrets as one unit. The legacy pre-isolation bundle has a separate retirement decision and must never be rebound into this cycle.

The public deployment has one origin, `anchorage.proofoftech.org`, because the
OAuth callback is registered for exactly that origin. The SPA publishes
indexable metadata, `robots.txt`, a sitemap, and a 1200×630 social card for it.

> **Do not paste the demo tokens in as that secret.** They are checked into this
> repository, so seeding them publishes world-known credentials — one of which is
> `admin`, who can both file and decide approvals. Generate random tokens
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

The default in-memory idempotency store is created fresh with the runtime, so
the showcase acknowledges the v2 key boundary automatically. If a host injects
a durable `idempotencyStore`, it must also set
`idempotencyKeyMigration: 'legacy-writers-drained'` only after every older
Breakwater writer sharing that store has stopped and drained. The injected
store must implement both atomic reservation and non-mutating legacy
inspection.

## Verify

```bash
pnpm --filter showcase typecheck   # browser + worker + node tsc passes
pnpm --filter showcase test        # worker.e2e + workflows.e2e + the SPA suites
```
