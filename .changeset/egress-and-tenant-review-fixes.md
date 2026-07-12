---
'@proofoftech/breakwater': patch
'@proofoftech/flowsafe': minor
---

Reviewed cleanup batch across the egress guard, tenant-id primitives, and approval self-decision paths - no observable contract changes and all 1119+ tests preserved.

breakwater (patch): the egress host matcher and the allowlist validator are each a single shared definition (domainAllowed + assertEgressHostList, both driven by the one egressDomainAllowed match semantics), the normalized allowlist is computed once per construction instead of per hop, and the per-connector egress guard is built once at createConnector. egressFetch also treats an async-iterable (Node Readable) request body as one-shot so a 307/308 redirect no longer re-sends a consumed body, validates maxRedirects at construction, and fails closed on a browser opaque status-0 redirect response.

flowsafe (minor): the tenant-salted ownership predicate and the id-mint rigor are hoisted into tenantOwnsSaltedId / assertMintableTenantId / mintSaltedId in do-runner/path-safe-id, and every live copy (runId and memory ownership, plus the approval write-path INV-1 belt) routes through them; mintSaltedId validates the tenant before evaluating a lazy suffix, so a caller-supplied uuid callback (mintThreadId's) can no longer run its side effects or throw ahead of the INV-3/reserved rejection. purgeTenant runs its three agent-memory deletes concurrently. The self-decision policy is threaded through createTenantResolver so TenantContext.canSelfDecide(role) is the single display hint the /workflows echo reads, and parseSelfDecision is memoized per deployment value. TenantContext gains a required canSelfDecide(role) member, BREAKING for hand-built TenantContext implementations (contexts from createTenantResolver get it automatically), hence the minor bump.
