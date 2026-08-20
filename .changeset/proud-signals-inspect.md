---
'@proofoftech/breakwater': minor
'@proofoftech/flowsafe': minor
---

Add an optional content-policy boundary for agent signals. Breakwater exposes `createContentPolicyGate()`, a reusable opaque input-policy gate for host code outside Mastra's processor chain, and FlowSafe's thread signal routes accept a structural `contentPolicy` callback that inspects Mastra's canonical escaped XML before delivery, persistence, wake, or run start — covering direct ingestion, providers, schedules, and notification dispatch. Denial is terminal and evaluator failure stays recoverable on every lane; neither exposes policy names, reasons, content, or causes.

Signal attributes whose keys are not XML names are now dropped when a signal is ingested, and a schedule whose stored target cannot be rendered settles a terminal discard receipt instead of failing every later tick with the same broken target.

Provider deliveries now distinguish a terminal refusal from one the deployment could not decide: an undecided webhook is answered with 503 so the sender redelivers, and every delivery carries a dedupe key derived from the signed bytes and the subscription so a redelivery coalesces into a still-pending notification instead of duplicating it. Webhook and poll results report `denied`, `failed`, and `deferred` counts.
