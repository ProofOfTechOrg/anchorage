---
'@proofoftech/breakwater': minor
---

Add stable connector decision codes, canonical policy categories, retryability and safe structured details to authored errors and audit events. Preserve custom policy names and the three-string policy-error constructor.

Wrap store and evaluator failures with typed errors. Pre-execution store failures retain their original cause at the base connector boundary; Agent CLI adapters preserve the classification without exposing raw causes. Post-effect commit and best-effort release failures retain their existing suppressed disposition and non-retryable audit codes. Direct invocation and validation errors gain stable tags, and egress refusals retain their transport checks.

Contain audit error-observer failures so they cannot replace completed connector results, release a pending reservation after failed result storage, or replace an original execution error. Rejected observer promises stay isolated too.
