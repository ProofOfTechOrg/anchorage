---
'@proofoftech/fleet-control': minor
---

Add `CloudflareApiPlainWorkerBackend`, a direct Cloudflare API backend for platform-authored ordinary Workers. Its adapter classifies each mutation's dispatch under the operation's own execution context, so a queued mutation's pre-dispatch failure rejects instead of resolving `{ status: 'failed' }`. Construct it with a plain-only `CloudflareProvisioningClient`. Configure that client with a shared rate coordinator and a durable export store. The existing Wrangler and Workers for Platforms backends keep their public provisioning contracts.

Expose the configured provider request timeout through the public `CloudflareProvisioningClient.requestTimeoutMs` getter.

Harden provider behavior across the built-in backends:

- **BEHAVIOR CHANGE:** Disable Cloudflare SDK logging even when `CLOUDFLARE_LOG` requests verbose output.
- **BEHAVIOR CHANGE:** Bound every paginated Cloudflare inventory and fail instead of truncating an over-bound result.
- **BEHAVIOR CHANGE:** Disable SDK retries for Worker upload and deployment, D1 creation and query, and each D1 export poll. This includes the D1 query path shared by both provider backends.
- **BEHAVIOR CHANGE:** Redact signed URLs, provider response details, headers, and original causes from database export failures.
- **BEHAVIOR CHANGE:** Replace uploaded secret plaintext in **ordinary-Worker** provider error messages, and in the cause chain of transport failures behind them, before returning a failed mutation outcome.
- **BEHAVIOR CHANGE:** Surface a lost lease before database or R2 reconciliation in all three affected create paths.
- **BEHAVIOR CHANGE:** Reject `PlainWorkerBackend.identityCaller` values that are not printable, single-line ASCII tokens from 1 through 128 characters.
- **BEHAVIOR CHANGE:** Refuse a reconciled Worker upload whose workers.dev or preview-URL state does not match the intent instead of accepting it by tag rediscovery.
- **BEHAVIOR CHANGE:** Queued provider mutations assert their own lease. Under concurrency pressure a queued mutation previously ran under the preceding operation's execution context.

When upgrading, construct direct ordinary-Worker clients with `plane: 'plain-worker'` and no `dispatchNamespace`. Keep `dispatchNamespace` on Workers for Platforms clients, provide one quota coordinator across every replica sharing a provider token, and ensure the token can complete the documented attachment scans before destructive teardown.
