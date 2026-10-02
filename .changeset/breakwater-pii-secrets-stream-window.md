---
"@proofoftech/breakwater": patch
---

On streamed answer and reasoning text, a `piiSecrets` policy whose only detectors are `ssn` and/or `awsAccessKey` no longer denies an SSN- or AWS-access-key-shaped run that the incremental scan cut from a longer word, such as `INV123-45-6789`. With any other detector enabled, such a run still denies.

This change adds no new denials. No migration is needed.
