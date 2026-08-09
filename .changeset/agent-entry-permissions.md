---
'@proofoftech/flowsafe': minor
---

Add server-owned, fine-grained permission resolution to the guarded agent host.

Agents can declare `requiredPermissions` with all-of semantics. The thread host resolves effective permissions from the trusted human or automated principal, records the required identifiers and policy version in authorization audit detail, and denies execution when any permission is missing. Agents that use only `allowedRoles` and `allowedAutomation` keep their existing behavior and do not require or invoke a resolver.
