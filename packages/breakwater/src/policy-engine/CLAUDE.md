# Policy engine navigation

- `index.ts`: `PolicyEngine`, stream channels, hold-back, and basic policies
- `content-inspection.ts`: PII, secret, entropy, and classifier policies
- `evaluator-contract.ts`: phase, channel, context, and evaluator declarations shared by the evaluator modules
- `tool-policy.ts`: egress, approval, workflow, tenant, and background evaluators
- matching `*.test.ts` files: processor and evaluator coverage

See [`../../../../docs/policy-engine-design.md`](../../../../docs/policy-engine-design.md).
