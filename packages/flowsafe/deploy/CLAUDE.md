# deploy/

Reference production deployment — the copy-me Worker template wiring the DO
runner + approval queue with real auth, cron-owned SLA sweep + retention
purge, and multi-gate approval bridging. Not shipped in the package; consumers
copy it.

## Files

| File | What | When to read |
| ---- | ---- | ------------ |
| `worker.ts` | Production-shaped Worker — example gated workflow + its `WorkflowMeta`, `FlowsafeRunner` DO (grant store bound to the DO's own tenant), `createTenantResolver` (authenticate → INV-3 → bind) optionally wrapped in `withSubdomainCrossCheck` behind `TENANT_APEX_DOMAIN`, and TWO-cron `scheduled()` dispatch (SWEEP_CRON vs PURGE_CRON — a CPU-limit kill is un-catchable, so sweep and purge never share an invocation; an unrecognized expression runs both + logs). Auth, the RBAC gate order, the approval bridges, the DO-stub topology (`createDoRunTopology`), the service assembly (`buildHostApprovalService`), the isolate-scoped factory memo (`approvalStoreFactoryFor`), the SLA sweep runner, and the `queue()` audit-export handler all come from `@proofoftech/flowsafe/host-kit` / `audit-export` — not copied here | Changing the template's wiring, auth, maintenance, or audit-export behavior |
| `crons.ts` | `SWEEP_CRON`/`PURGE_CRON` — the two cron expressions the dispatch matches on. Own module because worker.ts is the workerd ENTRY module, and workerd rejects any entry-module export that is not a handler/class/function — exporting a bare const from worker.ts fails the Worker at startup (the e2e test needs to import them) | Changing cron cadence (keep byte-equal to wrangler.jsonc) |
| `worker.e2e.test.ts` | Executable proof for the template — drives the real handler's `fetch()`/`scheduled()`/`queue()` in-process (real Mastra D1Store + `D1ApprovalStore` over `node:sqlite`, real `FlowsafeRunner` behind a stub DO namespace): auth seam incl. the reserved-`system` drop, the full approval loop with SoD, fail-closed forged resume, tenant-boundary 400/403/404s, cron sweep+purge with isolated failures, and the audit-export consumer. Typechecks under `tsconfig.test.json`, not `deploy/tsconfig.json` | Changing the template's behavior, debugging a deploy e2e failure |
| `wrangler.jsonc` | Deploy config — `RUNNER` DO + `DB` D1 bindings, TWO cron triggers (keep byte-equal to crons.ts's `SWEEP_CRON`/`PURGE_CRON`), SLA/retention vars, commented `queues` block for audit export, secret docs | Changing bindings, cron cadence, or defaults |
| `tsconfig.json` | Template TS config; resolves `@proofoftech/flowsafe/*` specifiers (incl. `/host-kit`) to `../src` so the copy-ready imports typecheck in-repo | Debugging deploy typecheck failures |
| `README.md` | Deploy checklist, cron semantics, config table, encoded conventions | Deploying, operating, or copying the template |

## Verify

```bash
pnpm --filter @proofoftech/flowsafe test deploy/worker.e2e.test.ts  # the template's own e2e
pnpm --filter @proofoftech/flowsafe typecheck   # includes deploy/tsconfig.json
pnpm --filter @proofoftech/flowsafe deploy:dev  # local workerd, no CF account
```
