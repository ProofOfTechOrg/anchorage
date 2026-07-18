---
'@proofoftech/flowsafe': patch
---

Harden the P6 ingestion routers against a pre-auth malformed-path fault. The
signal, goal, and schedule routers decoded the threadId/schedule-id path
segment with bare `decodeURIComponent` before authentication, and
`createFlowsafeWorker`'s fetch handler did not wrap the router calls — so an
unauthenticated request with malformed percent-encoding (e.g.
`POST /api/threads/%/message`) threw a `URIError` out of `fetch()` as a
per-request 500. The three routers now use a shared `safeDecodeSegment`
(host-kit) that treats malformed encoding as route-absent (byte-identical to a
non-matching path), matching the Track E webhook router; the worker fetch
handler gains a top-level try/catch that contains any handler throw as a
generic 500 without leaking `error.message`. The same helper closes the whole
class: the background-tasks read route (post-auth, DO-mounted) adopts it too, so
a malformed taskId returns the no-oracle 404 instead of throwing. Additive and
behavior-preserving for all valid paths.
