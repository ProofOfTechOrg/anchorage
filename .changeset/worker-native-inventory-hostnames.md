---
'@proofoftech/fleet-control': patch
---

Use the platform URL hostname parser for inventory finding validation so the ordinary Worker control plane does not require node:url. Preserve international hostname validation, rejected-input behavior and original diagnostic text.

Use crypto.randomBytes results directly for R2 reservation nonces and deployment-secret encoding, preserving their byte lengths and base64url representation.
