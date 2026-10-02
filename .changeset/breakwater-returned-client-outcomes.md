---
"@proofoftech/breakwater": patch
---

A returned message Mastra holds as both remembered and call input stays both when an application input processor returns an array or a `{ messages, systemMessages }` pair.

This closes a security gap where the client tool result a caller sends for a remembered assistant message reaches the model without input policy evaluation, and could leave the stored tool call pending, when an application input processor returns its messages, as Mastra's `UnicodeNormalizer` does.

Such a processor's change to that message is now saved, as when it returns the message list. A mapper error on a stored result in a message it changes stops the call with `input processor failed`. No migration is needed.
