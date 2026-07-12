---
'@proofoftech/breakwater': patch
---

`egressFetch` now releases each intermediate redirect response before following it or throwing. The manual redirect follower cancels the discarded 3xx's body stream, so a followed, hop-capped, egress-denied, or one-shot-refused redirect can no longer retain its connection until GC (Node/Undici, workerd) under sustained redirected traffic. Disposal is best-effort (a locked/errored stream's cancel rejection and an injected transport's synchronous throw are both swallowed) and never touches the response returned to the caller.
