---
"@proofoftech/breakwater": patch
---

A guarded agent maps, through `toModelOutput` and before the input policies, a client-only tool result in a message an application input processor changes. For example, a processor can merge the result into a remembered assistant message. The policies read the mapped output the model receives.

This closes a security gap where mapped output reaches the model without input policy evaluation.

A mapper error on any result in such a message, stored results included, stops the call with `input processor failed` and an `agent.input.processor` error event naming `breakwater-client-tool-output`. The policies read such a message once more in its mapped version, which `maxTextLength` counts. No migration is needed.
