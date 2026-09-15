---
'@proofoftech/fleet-control': patch
---

Refuse redirects on the three credentialed provider transports. `CloudflareProvisioningClient`, `PlainWorkerBackend`, and `WorkersForPlatformsBackend` force `redirect: 'manual'` after a caller's `init`, so a bearer credential is not carried to an address the control plane did not choose. The raw dispatch script page and both Workers for Platforms maintenance calls, which read a response the transport does not otherwise classify, throw `CredentialedRedirectRefusedError` on a 301, 302, 303, 307, or 308 and cancel the unconsumed body; the message names the operation and the status, never the address. An SDK-routed redirect surfaces as an `APIError` with the original status, unretried and not classified transient.

A host whose injected fetch follows redirects itself is unaffected: supply that fetch only from trusted control-plane code.
