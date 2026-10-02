---
"@proofoftech/breakwater": minor
---

Every `PolicyEngine` policy denial uses the tripwire reason `policy '<name>' denied the input|output`. This reason appears in `result.tripwire.reason`, the stream's `tripwire` chunk on both Mastra agent loops, and Mastra's logs and spans.

When a policy evaluator throws or returns no decision at the final result, `PolicyEngine` throws a plain `Error` with the fixed message `policy evaluation failed` and no `cause`. Mastra's standard loop surfaces it to the `generate()` caller; the durable loop's finish step runs output processors but only logs what they throw. A missing decision no longer surfaces as `TypeError('policy evaluator returned no decision')`, so hosts checking `instanceof TypeError` stop matching.

Use `policyDenialReason(policyName, phase)`, exported from both `@proofoftech/breakwater` and `@proofoftech/breakwater/policy-engine`, to construct the denial reason.

Security: For `PolicyEngine` and `createContentPolicyGate` denials, evaluator reasons, classifier echoes, configured patterns, length limits, and detector names reach neither the caller nor the policy audit record. Audit events identify the policy in `detail.policy` and use static denial or failure reasons.

Migration: Hosts that parse denial reasons match `policyDenialReason(...)`, read the audit event's `detail.policy`, or record the diagnostic information they need inside their own evaluator.
