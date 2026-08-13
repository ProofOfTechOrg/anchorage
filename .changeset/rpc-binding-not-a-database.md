---
'@proofoftech/flowsafe': patch
---

Stop treating an RPC binding as the deployment database.

`isDatabaseBinding` accepted any binding whose `prepare` was a function. A
service binding with a named `entrypoint`, and a Durable Object stub, are
proxies that answer every property with a callable, so the deployment-sentinel
scan adopted them as databases and the request failed with `The RPC receiver
does not implement the method "prepare"`.

Any Worker holding both a `DB` binding and an RPC binding was affected. Fleet
control's trusted state scripts are exactly that shape — `DB` beside
`OUTBOUND_PROXY` bound to the shared outbound Worker's `StateEgress`
entrypoint — so they failed on the first Durable Object request they served.
Fetcher-shaped bindings are now excluded; `D1Database` has no `fetch`.
