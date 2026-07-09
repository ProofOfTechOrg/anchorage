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
| `worker.ts` | Host only — `ShowcaseRunner` DO, cron `scheduled()`, optional Queues audit export, and the DO-stub start/status/resume thunks it hands to host-kit's `createRunRouter`. Auth, the RBAC gate order, and the approval bridge come from `src/host-kit`; workflows from `buildShowcaseRuntime` | Changing the DO topology, maintenance, or bridge wiring |
| `demo-actors.ts` | The demo bearer identities — ONE source for the UI switcher, the `app:dev` host, and `.dev.vars.example`. Dependency-free (no approval-api import) so both the DOM and workers-types tsconfigs can load it | Changing the demo identities |
| `demo-actors.test.ts` | Drift guard: `.dev.vars.example` ↔ `demo-actors.ts`, and `DemoRole` ↔ `APPROVAL_ROLES` | Changing the demo identities or the dev-vars file |
| `worker.e2e.test.ts` | In-process guard for gtm-outbound (approve => grant minted => connector runs simulated; forged => denied) | Debugging the gtm grant/simulate interaction |
| `workflows.e2e.test.ts` | Per-workflow guards for the other 4 (content R2 write, lead branch routing, product 2-gate SoD, access-request grant + cross-workflow-isolation denial), plus the run-route guards over the real module metas | Adding/debugging a workflow module |
| `wrangler.jsonc` | Deploy config — `RUNNER` DO + `DB` D1, cron, the `assets` block (single-deploy: serves `../app/dist` at `/`, `run_worker_first` keeps the API on the same origin), commented `send_email` + `queues`. Carries NO credentials: `APPROVAL_ACTOR_TOKENS` is a secret, so a deploy without it 401s everywhere | Changing bindings or the assets/single-deploy config |
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
