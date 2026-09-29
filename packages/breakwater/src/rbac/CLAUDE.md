# RBAC navigation

- `index.ts`: actor request-context lookup, `RBACMiddleware`, and the public re-exports
- `actor.ts`: the actor and role declarations RBAC authorizes and audit attributes
- `roles.ts`: the role vocabulary and the shared role-allowlist reader
- `principal.ts`: principal kinds and the shared kind-allowlist validator
- `authorize.ts`: the shared actor-authorization gate
- `rbac.test.ts`: authorization and audit coverage

Authentication remains a host responsibility. See [`../../../../docs/breakwater-purpose-and-boundaries.md`](../../../../docs/breakwater-purpose-and-boundaries.md).
