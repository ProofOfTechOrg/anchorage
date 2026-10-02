---
"@proofoftech/breakwater": patch
---

The input policies read a remembered message carrying the caller's client tool outcome that an application input processor keeps but moves out of the call's input through `MessageList` methods while returning the list or nothing. A refused call removes that message, also when the processor aborts or throws after moving it.

This closes a security gap where the client tool result reached the model without input policy evaluation, and a move to `response` could also save it to memory.

The moved message is read whole, stored history included, so a policy hit there refuses the call, as for a history message a processor rewrites. Its results get no new `toModelOutput` mapping. No migration is needed.
