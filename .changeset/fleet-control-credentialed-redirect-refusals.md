---
'@proofoftech/fleet-control': patch
---

Refuse redirects on the three credentialed provider transports. `CloudflareProvisioningClient`, `PlainWorkerBackend`, and `WorkersForPlatformsBackend` force `redirect: 'manual'` after a caller's `init`, so a bearer credential is not carried to an address the control plane did not choose. The raw dispatch script page refuses a redirect status where it reads the raw response, and the Workers for Platforms maintenance transport refuses one in its wrapper, because its callers read only the signed receipt header. Each throws `CredentialedRedirectRefusedError` on a 301, 302, 303, 307, or 308 and cancels the unconsumed body; the message names the operation and the status, never the address. An SDK-routed redirect surfaces as an `APIError` with the original status, not retried on the status alone and not classified transient. The Cloudflare SDK obeys an `x-should-retry: true` response header ahead of the status, so a redirect carrying that header is retried; each attempt goes through the same forced `redirect: 'manual'`, so no attempt follows the redirect.

A host whose injected fetch follows redirects itself is unaffected: supply that fetch only from trusted control-plane code.
