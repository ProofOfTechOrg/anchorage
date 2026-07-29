---
"@proofoftech/breakwater": minor
"@proofoftech/flowsafe": minor
---

Replace connector ID approval arrays with structured connector grants. Durable-agent approvals now bind to the exact Mastra tool call, workflow approvals bind to the exact suspension, and standing grants require explicit run scope.

This is intentionally breaking: `APPROVED_CONNECTORS_CONTEXT_KEY`, `BREAKWATER_APPROVED_CONNECTORS_KEY`, and `approvedConnectorsForLeg()` are removed. Legacy arrays and approval rows without explicit scope fail closed. Migrate trusted hosts to `CONNECTOR_GRANTS_CONTEXT_KEY`, `CONNECTOR_EXECUTION_CONTEXT_KEY`, and `connectorGrantsForLeg()`.
