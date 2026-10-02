---
"@proofoftech/flowsafe": patch
---

A guarded `resumeViaRuntime()` whose memory does not resolve is refused before registry installation, observation, or the resumed step. The run stays suspended and can be redriven once memory resolves. A failure while Breakwater resolves the memory processors, including a memory processor lookup error or function-valued memory that resolves a title-enabled `Memory` there, stops the audited `breakwater-memory` step with `input processor failed` and one `agent.input.processor` error event; the resume rejects with `Durable agent registry rehydration denied: input processor failed`. If durable preparation's first memory lookup throws or resolves a title-enabled `Memory`, the resume rejects with that error, including the title `TypeError`, and writes no audit event.

Security: Before this change, such a resume ran the approved tool and failed only at the next model step. A leg with no further model step finished and saved through the memory.

No migration is needed.
