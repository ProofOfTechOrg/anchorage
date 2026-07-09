# deploy/

Reference production deployment — the copy-me Worker template wiring the DO
runner + approval queue with real auth, cron-owned SLA sweep + retention
purge, and multi-gate approval bridging. Not shipped in the package; consumers
copy it.

## Files

| File | What | When to read |
| ---- | ---- | ------------ |
| `worker.ts` | Production-shaped Worker — example gated workflow + its `WorkflowMeta`, `FlowsafeRunner` DO, the DO-stub `start`/`status`/`resume` thunks handed to host-kit's `createRunRouter`, `scheduled()` running `sweepSLA` + `purgeExpiredWorkflowRuns`, optional audit export (`AUDIT_QUEUE` producer sink + `queue()` SIEM consumer). Auth (`bearerActorAuthenticator`), the RBAC gate order, and the approval bridges come from `@proofoftech/flowsafe/host-kit` — not copied here | Changing the template's wiring, auth, maintenance, or audit-export behavior |
| `wrangler.jsonc` | Deploy config — `RUNNER` DO + `DB` D1 bindings, cron trigger, SLA/retention vars, commented `queues` block for audit export, secret docs | Changing bindings, cron cadence, or defaults |
| `tsconfig.json` | Template TS config; resolves `@proofoftech/flowsafe/*` specifiers (incl. `/host-kit`) to `../src` so the copy-ready imports typecheck in-repo | Debugging deploy typecheck failures |
| `README.md` | Deploy checklist, cron semantics, config table, encoded conventions | Deploying, operating, or copying the template |

## Verify

```bash
pnpm --filter @proofoftech/flowsafe typecheck   # includes deploy/tsconfig.json
pnpm --filter @proofoftech/flowsafe deploy:dev  # local workerd, no CF account
```
