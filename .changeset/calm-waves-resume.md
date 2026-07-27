---
'@proofoftech/flowsafe': patch
---

Prevent approval resume after isolate eviction from rerunning application input processors or input policy evaluation. Durable-agent recovery now reauthorizes the stored principal, restores both Mastra run registries with complete runtime processor chains, and fails before resumed tool execution when authorization is denied.
