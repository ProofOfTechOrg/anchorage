---
"@proofoftech/breakwater": minor
"@proofoftech/flowsafe": minor
---

Fail closed on structured-output coverage gaps. `createGuardedAgent()` rejects structured output before model execution because Mastra exposes parsed values to messages, persistence, and observability hooks before a post-generation wrapper could inspect them. It also rejects object-only policies that no supported guarded invocation can cover.

Processor-visible object chunks are validated as JSON, evaluated through their canonical serialization, and replaced with the same canonical clone. Standalone object-only policies abort when an invocation exposes no object to the processor. Policy lists and decision-driving descriptors are snapshotted at construction, evaluator callables retain their original receiver, and per-stream audit metadata stays bounded by configured policies and channels.

Flowsafe recognizes the new guarded-agent host protocol and rejects structured output on durable stream, generate, and prepare before Mastra can bypass the narrow handle. Durable entry points snapshot data-property call options before validation and delegation, reject accessors, and use the same snapshot for later run registration. Both packages pin their tested `@mastra/core` 1.50.0 contract.

Hold-back cost under large streams is measured by opt-in evidence tests (`BREAKWATER_PERF=1`) and recorded in the policy-engine design guide.
