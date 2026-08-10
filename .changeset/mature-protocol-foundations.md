---
"@proofoftech/flowsafe": minor
---

Use `jose` for actor JWT verification and stream-ticket signing. Stream tickets are now standard three-segment JWTs with a dedicated audience and `typ`, so actor tokens and stream tickets cannot cross-verify even if a deployment reuses a secret. Existing verifier injection, actor validation, issuer, audience, expiry, and key-selection behavior remains fail closed.

GitHub webhook signatures now require the exact `sha256=<64 hex>` shape before raw-byte WebCrypto verification. Malformed signatures and invalid UTF-8 JSON return stable client errors without throwing, while empty and non-ASCII payloads remain byte-exact.

Deployment identity provisioning now recognizes Cloudflare D1's exact `_cf_KV` and `_cf_METADATA` internal tables on a fresh database while continuing to reject arbitrary or lookalike pre-existing application tables.
