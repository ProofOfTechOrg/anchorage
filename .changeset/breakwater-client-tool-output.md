---
"@proofoftech/breakwater": minor
---

A guarded agent maps a replayed client-only tool result with the tool's `toModelOutput` during input processing, after application input processors and the input asset check. The mapper runs once per result part, and input policies read the mapped output the model receives. The reserved processor id `breakwater-client-tool-output` is refused for application processors.

`toModelOutput` now runs before the client tool's `onOutput` hook, a behavior change hosts can observe. A mapper, tool-resolution, or mapped-output normalization error stops the call with `input processor failed` and one `agent.input.processor` error event whose detail names `breakwater-client-tool-output`. URLs inside mapped tool results are not checked against `allowedInputAssetOrigins`; a model provider may fetch them.

Security: before this change, a client-only tool's mapped output reached the model without input policy evaluation, and a mapper error let the raw result reach the model.
