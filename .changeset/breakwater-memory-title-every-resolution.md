---
"@proofoftech/breakwater": patch
---

A guarded agent checks every memory resolution for thread title generation, so a title-enabled function-valued or inherited `Memory` is refused wherever Mastra resolves it. At the `generate()`/`stream()` entry check, the standard loops' execution lookup, and the durable loop's first lookup, the call rejects with the `generateTitle` `TypeError` before the model runs and writes no audit event. A title-enabled `Memory` resolved for the memory processors still stops the call with `input processor failed` and an audited `breakwater-memory` error. A lookup at Mastra's finish runs after the model. Before this change, a durable call whose first lookup alone resolved a title-enabled `Memory` answered.

Security: Before this change, the durable loop's first memory resolution was not checked, even though that `Memory` backs message saving. Thread title generation itself stayed disabled.

Migration: Disable `generateTitle` on every `Memory` a guarded agent can resolve.
