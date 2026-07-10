# showcase/

The Anchorage **showcase** — all five example workflows made runnable behind one
frontend and shipped as a single Cloudflare deploy (one Worker serving both the
API and the SPA). Grew out of the original single-workflow `gtm-app`. Each
workflow's side effect is **binding-gated**: with no binding a connector
simulates its effect (envelope/preview logged, zero secrets) while STILL running
the real `execute`, so the approval-grant gate is exercised and a forged resume
fails closed. content-pipeline's R2 write is offline-real via the in-memory
artifact bucket.

The five workflows and what each showcases:

| id | shape | capabilities |
| -- | ----- | ------------ |
| `gtm-outbound` | serial + 1 gate | write-approval grant, binding-gated Cloudflare Email Service send, audit |
| `content-pipeline` | `.parallel()` → gate → publish | parallel fan-out/fan-in, R2ArtifactStore write, content-hash idempotency |
| `lead-generation` | `.branch()` hot/cold → gate → assign | conditional branch, egress allowlist + rate-limit on the CRM write |
| `product-launch` | serial, **2 gates** | multi-checkpoint re-suspension, destructive + idempotent deploy, dry-run pre-flight |
| `access-request` | serial + gate, RBAC-scoped | route-level `allowedRoles`, `crossWorkflowIsolation`, separation of duties |

## Files

| File | What | When to read |
| ---- | ---- | ------------ |
| `runtime.ts` | `buildShowcaseRuntime(deps)` — one `init()`, registers all 5 modules on one runtime; resolves optional `ShowcaseDeps` → concrete stores. Exports `SHOWCASE_MODULES` (the module list + metas) | Changing which workflows are hosted, the deps resolution, or the grant seam |
| `workflows/` | The 5 workflow modules + `shared.ts` (`ShowcaseDeps`/`ShowcaseModuleDeps`, `callConnector` context-injection helper) | Changing a workflow's steps/connector, or the connector-call helper |
| `worker.ts` | Host only — `ShowcaseRunner` DO (grant store bound to the DO's OWN tenant via `this.tenantId`), TWO-cron `scheduled()` dispatch (SWEEP_CRON vs PURGE_CRON — a CPU-limit kill is un-catchable, so they never share an invocation; purge = retention + demo-tenant reaper), `buildVerifier` (static map + demo JWTs, kill-switch aware), the demo-budget charge on run starts, the `/auth/*` mount with env-selected provider (Google when its FULL id+secret pair is set, else GitHub's pair — one per deployment; a half-set pair is a config-error and stays unmounted), and the DO-stub thunks for `createRunRouter({ resolve })` | Changing the DO topology, maintenance, auth composition, or bridge wiring |
| `demo-actors.ts` | The LOCAL-dev bearer identities (tenant `demo`) — ONE source for the dev switcher, the `app:dev` host, and `.dev.vars.example`. Dependency-free so both tsconfigs load it. PUBLIC-demo identities are OAuth-minted at runtime (demo-auth.ts), never literals | Changing the local demo identities |
| `demo-auth.ts` | The public demo: OAuth sign-in (`OAuthProvider` seam — `googleProvider` is the launch provider (form-encoded token exchange, `openid`-only scope, stable `sub` subject), `githubProvider` the fallback; state is HMAC-signed AND bound to an HttpOnly nonce cookie — a signed state alone is login-CSRF-replayable) → ephemeral tenant via the allocation registry + `demo_tenants` (UNIQUE(provider,subject); `INSERT OR IGNORE` + read-back so concurrent first sign-ins resolve to one tenant; expired re-auth purges inline then mints fresh) + four-role HS256 JWT set (distinct actor ids — SoD stays demonstrable); `isDemoTenant` reads `tenants.kind` (never a slug prefix); ATOMIC per-tenant + global-daily run budgets (single conditional UPDATEs, no TOCTOU; refusals carry `DemoRunLimitError.{scope,reason}` — machine-readable `not-provisioned`/`expired`/`cap-reached`, never string-match `message`); `purgeExpiredDemoTenants` takes a REQUIRED `graceMs` (a refresh at the last live instant mints a full-TTL token, so reaping at `expires_at` would delete runs under a valid token), is LIMIT-batched with the shrinking table as its cursor, and isolates failures PER TENANT (a wedged purge keeps its row and the pass continues — oldest-first would otherwise head-of-line block every later expiry; failures re-throw aggregated after the pass); kill switch honored at mint AND (via `worker.buildVerifier`) on already-issued JWTs | Changing demo sign-in, caps, or sandbox lifecycle |
| `demo-auth.test.ts` | Tenant lifecycle (incl. the concurrent-first-sign-in race), atomic caps + daily ceiling, `isDemoTenant` vs a commercial `dm*` slug, the purge grace window, batched reaper + per-tenant failure isolation (a wedged tenant must not block the rest), `googleProvider` (code-flow authorize URL, FORM-encoded token exchange, Bearer userinfo, `google:`-scoped subject, fail-closed on every step), state forgery + login-CSRF (validly-signed state without the browser cookie), token set, full fake-provider round-trip, and the kill-switch-kills-issued-JWTs pin | Changing any of the above |
| `demo-actors.test.ts` | Drift guard: `.dev.vars.example` ↔ `demo-actors.ts`, and `DemoRole` ↔ `APPROVAL_ROLES` | Changing the demo identities or the dev-vars file |
| `worker.e2e.test.ts` | In-process guard for gtm-outbound (approve => grant minted => connector runs simulated; forged => denied) | Debugging the gtm grant/simulate interaction |
| `worker.fetch.e2e.test.ts` | Drives the EXPORTED handler's `fetch()` in-process (D1-shaped node:sqlite, throwing DO stub — these routes never reach a run leg): healthz + 401 fall-through, catalog actor echo over the real metas, and the demo-auth mount matrix (demo-off half-pair = silent + unmounted; demo-on half-pair = config-error + unmounted; full Google pair = authorize redirect + state cookie; half Google + full GitHub = fallback mounts) | Changing fetch() route composition or the auth mount |
| `workflows.e2e.test.ts` | Per-workflow guards for the other 4 (content R2 write, lead branch routing, product 2-gate SoD, access-request grant + cross-workflow-isolation denial), plus the run-route guards over the real module metas | Adding/debugging a workflow module |
| `wrangler.jsonc` | Deploy config — custom domain `anchorage.proofoftech.org` + `workers_dev: false` (single public origin: the Google OAuth callback is registered for exactly that origin), `RUNNER` DO + `DB` D1, cron, the `assets` block (single-deploy: serves `../app/dist` at `/`, `run_worker_first` keeps the API on the same origin), commented `send_email` + `queues`. Carries NO credentials: `APPROVAL_ACTOR_TOKENS` is a secret, so a deploy without it 401s everywhere | Changing bindings, the public origin, or the assets/single-deploy config |
| `tsconfig.json` | Workers-types build config; breakwater resolves to source, flowsafe via relative `../src` | Debugging showcase typecheck failures |
| `.dev.vars.example` | Local-dev secrets (`cp` to `.dev.vars` — REQUIRED for `showcase:dev`): the demo `APPROVAL_ACTOR_TOKENS` map, plus the optional SIEM header | Running `showcase:dev`, or wiring real secrets |
| `README.md` | curl script, single-deploy notes, the fail-closed secret posture, going-live per connector | Running or extending the showcase |

## Run (dev)

```bash
pnpm --filter @proofoftech/flowsafe build     # showcase:dev bundles breakwater from dist
pnpm --filter @proofoftech/flowsafe showcase:dev   # wrangler dev (:8787); build app first for the SPA
```

## Single deploy (SPA + API, one Worker)

```bash
pnpm --filter @proofoftech/flowsafe app:build            # produces ../app/dist (served as assets)
pnpm --filter @proofoftech/flowsafe showcase:dev         # verify locally at /
pnpm --filter @proofoftech/flowsafe showcase:deploy      # builds + wrangler deploy
```

## Verify

```bash
pnpm --filter @proofoftech/flowsafe typecheck   # includes showcase/tsconfig.json
pnpm --filter @proofoftech/flowsafe test         # includes worker.e2e + workflows.e2e
```
