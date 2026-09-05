---
"@proofoftech/fleet-control": patch
---

Validate plain Worker upgrades against the target application's bindings during candidate inspection, promotion, and settlement. Changing application variables or secrets no longer deploys the new bindings and then rejects them against the previous deployment's binding configuration.
