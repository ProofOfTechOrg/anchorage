# rbac/

## Files

| File | What | When to read |
| ---- | ---- | ------------ |
| `index.ts` | `RBACMiddleware` processor, `ROLES` (admin/builder/operator/reviewer/viewer), `actorFromRequestContext` + `ACTOR_CONTEXT_KEY`; re-exports the audit surface from `../audit` for compat | Changing roles/permissions or actor sourcing |
| `rbac.test.ts` | RBAC authorization tests (AuditLogger's own tests live in `../audit/audit.test.ts`) | Adding RBAC tests, debugging an allow/deny decision |
