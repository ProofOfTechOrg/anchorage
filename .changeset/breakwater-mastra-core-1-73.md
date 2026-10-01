---
"@proofoftech/breakwater": minor
---

Require `@mastra/core` `1.73.0` exactly. The Node.js runtime floor remains `22.13.0`.

On a thread with stored memory, Mastra keeps input after the last assistant message, or that message's trailing client tool outcomes when the input ends with an assistant message. A caller message reusing a stored id takes the stored copy; only client tool outcomes that advance a stored pending call are merged, not replacement text. This happens before input policies, so dropped caller content reaches neither the model nor the saved thread and produces no tripwire or audit event. Send the new turn rather than re-sending history.

Security: Results, errors and denials a caller sends for a stored pending client tool call under the stored assistant message's id are evaluated as caller input after memory merges them. Breakwater records client tool outcomes before memory processing and evaluates their merged parts. Stored outcomes not re-sent are not re-evaluated; those re-sent in the same state are. Two client tool outcomes for one tool call in a caller message stop with `input processor failed`. A standalone policy engine on a plain Mastra agent with memory evaluates every client tool outcome of a merged message, including stored ones.

Guarded agents disable Mastra's default error processors, so provider-history rewriting, prefill retry and transient stream retry do not run. Construction refuses the error-processor options described in the [application processor rules](https://github.com/ProofOfTechOrg/anchorage/blob/main/packages/breakwater/README.md#application-processors). Transient stream failures and prefill rejections receive no automatic retry from those processors.

Guarded `stream()` disables eager tool execution: server tools wait until the step's stream-phase policies judge it. The durable loop runs tools after the model step regardless.

Breaking: Application input and output processors implementing `processToolResult` are refused at construction because Mastra runs that hook after Breakwater's policies on both standard and durable loops.

Migration: Move `processToolResult` logic from application input and application output processors into a tool's own result handling or an allowed processor hook. Upgrade `@mastra/core` to `1.73.0`.
