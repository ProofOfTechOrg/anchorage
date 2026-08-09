---
'@proofoftech/breakwater': minor
---

Add optional connector invocation authorization.

`PermissionManifest.requiredPermissions` declares an all-of list of canonical permission identifiers, validated at construction. The compiled execute path enforces it against the trusted `breakwater.principalPermissions` request-context projection before the dry-run branch and before approval-grant consumption, so a simulation still needs an authorized principal and a valid approval cannot elevate an unauthorized one. A missing, null, or malformed projection fails closed. A pass records a new `connector.authorize` audit event; it and the `required-permissions` denials record the required identifiers and the policy snapshot version, never the effective permission set.

The `rbac` subpath now owns the shared permission vocabulary: `Permission`, `isPermissionIdentifier`, the `PrincipalPermissions` projection type, its `isPrincipalPermissions` guard, and `PRINCIPAL_PERMISSIONS_CONTEXT_KEY`.
