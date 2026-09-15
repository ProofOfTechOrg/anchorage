---
'@proofoftech/flowsafe': minor
---

Accept optional non-reserved `requestContext` on authenticated run starts and carry it through the protected Durable Object topology into stored application context. Expose the validated value to router and Worker start-policy hooks while preserving shorter hook signatures.

Reject malformed context and reserved keys with HTTP 400. Verified schedule targets retain precedence, including absent context; provider application values override stored values and trusted capabilities retain their authority. Application context survives resume. Keyed replay validates input and runs host policy again, then preserves the first writer's context without comparing or overwriting it.

Correct public agent status, stream and ordinary termination lookups to return not found for a coherent snapshot belonging to another agent or thread. Private replay, proof and recovery retain strict failures so a foreign snapshot cannot be treated as absent.
