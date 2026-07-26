# Proposal: model gateway policy

> This document is a proposal. It is not implemented or supported product behavior. Shipped policy enforcement is documented in [Policy engine](../policy-engine-design.md) and [Connector interface](../connector-interface.md).

Mastra's `Agent` class handles model routing, provider normalization, and tool calling natively. This document covers the policy layer Anchorage adds on top of `Agent.generate()`.

## Pre-Gate Policies (Before Model Invocation)

| Policy | Description |
|---|---|
| Provider allowlist | Restrict which providers workflows can use |
| Budget check | Per-workflow or per-team spend limits in D1 (extends Mastra's `CostGuardProcessor`, which caps cost per run/resource/thread) |
| Network egress check | Verify target model endpoint is in allowlist |

## Post-Gate Policies (After Model Response)

| Policy | Description |
|---|---|
| Output schema validation | Verify response matches expected schema (Mastra's `StructuredOutputProcessor` covers the base case) |
| Content moderation | PII, injection, toxicity scanning (delegates to Mastra's built-in `PIIDetector`/`PromptInjectionDetector`/`ModerationProcessor`) |
| Quality scoring | Execute Mastra eval scorers on output |

## Relationship to Mastra Processors

The gates are breakwater's Mastra processors -- the RBACMiddleware/PolicyEngine chain from `breakwater-architecture.md`, registered via the agent's `inputProcessors`/`outputProcessors`. Mastra's built-in processors already cover prompt injection, PII redaction, content moderation, token limits, cost ceilings (`CostGuardProcessor`), and schema validation (`StructuredOutputProcessor`) at the agent boundary; the tables above list what Anchorage layers on top.

The gateway itself is a thin wrapper that owns `Agent.generate()` call sites. It exists because `generate()`/`stream()` accept per-call `inputProcessors`/`outputProcessors` options that override the agent's configured defaults -- a caller could strip the gates for a single call. The wrapper strips or merges those overrides so breakwater's processors are always present, and it hosts the call-level policies (provider allowlist, budget lookup) that need request context before the agent runs. Config-attached processors survive direct `agent.generate()` calls; the wrapper defeats override-stripping -- together they close both bypass paths.

## Budget Enforcement

Budget state lives in D1 with eventual consistency. Hard limits are checked before invocation; soft limits emit warnings. Budget records include provider, model, prompt hash, token count, and estimated cost.
