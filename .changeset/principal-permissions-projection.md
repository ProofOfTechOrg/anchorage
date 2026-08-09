---
'@proofoftech/flowsafe': minor
---

Project server-resolved principal permissions into trusted agent-run context.

The thread agent host now runs a configured `resolvePrincipalPermissions` on every authorized entry — role-only agents included — and mints the resolution into derived request context as `breakwater.principalPermissions` on every start and resume leg, where breakwater's connector `requiredPermissions` gate enforces it. When no resolution exists the host projects an explicit `null`, so a resume retires a stale persisted projection and permission-declaring connectors fail closed. A failed resolution still denies a permission-requiring agent; on a role-only agent it is audited as a new `agent.permissions.resolve` error event and the run proceeds without a projection.

`TrustedAgentExecution` gains a required `principalPermissions` field, `Permission` and `isPermissionIdentifier` are now re-exported from `@proofoftech/breakwater/rbac`, and the root/approval-api barrels export `BREAKWATER_PRINCIPAL_PERMISSIONS_KEY`. The optional `@proofoftech/breakwater` peer range moves to `>=0.9.0 <1.0.0` for the shared permission vocabulary.
