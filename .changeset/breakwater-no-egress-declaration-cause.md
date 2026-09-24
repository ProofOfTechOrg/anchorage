---
"@proofoftech/breakwater": patch
---

Correct the connector authoring guide on when the conformance harness reports `no-egress-declaration`: the guide now states that the cause applies to any successfully parsed host reached through a supplied base transport with no bound subject, which is the probe's transport at any time and a case's transport until its subject binds, not only while a factory is constructing.
