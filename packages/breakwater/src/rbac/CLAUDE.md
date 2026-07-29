# RBAC navigation

- `index.ts`: roles, actor request-context lookup, and `RBACMiddleware`
- `principal.ts`: principal kinds and the shared kind-allowlist validator
- `authorize.ts`: the one gate both the processor and direct calls run through
- `rbac.test.ts`: authorization and audit coverage

Authentication remains a host responsibility. See [`../../../../docs/breakwater-purpose-and-boundaries.md`](../../../../docs/breakwater-purpose-and-boundaries.md).
