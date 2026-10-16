---
'@proofoftech/flowsafe': minor
---

Add a D1-backed remote-step journal for agent and workflow suspensions, one-time host dispatch permits, retained results, and a host context derived from the stored invocation. Versioned private state egress carries internal dispatch metadata without changing the legacy route. A host-owned notification operation stores its effect and replay receipt in one D1 transaction with a configurable retained-receipt cap. General Mastra continuation remains fail-closed without generation-fenced snapshot writes; hosts must supply trusted dispatch transport and downstream effect receipts before enabling remote execution.
