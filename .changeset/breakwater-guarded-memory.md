---
"@proofoftech/breakwater": minor
---

The guarded durable loop loads the agent's memory, including thread history, working memory, and semantic recall, as the standard loops do. A memory resolution failure or memory input processor error, such as a failed thread-history read, stops a durable call with `input processor failed` and one `agent.input.processor` error event naming the processor's id (`message-history` for history). On `generate()` and `stream()`, Mastra rejects a memory input processor error and Breakwater writes no audit event. `createGuardedAgent()` refuses title generation on any `Memory` a call resolves, including function-valued and inherited memory. The guarded agent never generates a thread title, even when a host starts the durable loop with call-level `memory.options.generateTitle`.

Migration: Expect durable runs to see thread history and memory context. Disable title generation on any `Memory` the guarded agent resolves.

Security: Memory input processors, such as semantic recall's embedder, see the caller's text after RBAC and before the input policies on every loop. Mastra's thread title model call bypasses the guarded agent's input and output policies.
