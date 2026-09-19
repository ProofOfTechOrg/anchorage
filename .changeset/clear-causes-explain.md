---
"@proofoftech/breakwater": minor
---

`ConnectorConformanceEscape` gains a required `cause` discriminator. Code that constructs escape records must supply the refusal cause. `NETWORK_IO_OUTSIDE_RUNTIME_FETCH` findings from `policies.fetch` identify URL parsing, host parsing, a missing egress declaration, or an undeclared host as the refused check. Trapped entry points record `outside-runtime-fetch` and retain their existing finding text.
