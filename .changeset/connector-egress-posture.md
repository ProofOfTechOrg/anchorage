---
'@proofoftech/breakwater': minor
---

Add an egress posture to the connector manifest. `permissions.egressEnforcement` declares whether
the declared hosts bind the connector's actual traffic — `'enforced'` when every **HTTP** request
leaves through `ConnectorRuntime.fetch`, `'declaration-only'` when a vendor SDK or child process
carries its own transport. It is a claim about HTTP traffic, not about platform bindings (D1, KV, R2,
service bindings), which the guard never sees, so a connector that issues no HTTP request at all is
`'enforced'`. An omitted field resolves to `'declaration-only'`.

`connectorEgressPosture(tool)` reads the resolved posture beside `connectorManifest(tool)`, and every
connector audit event carries it as `detail.egressEnforcement`, so an operator can answer which
connectors have real egress enforcement from the log.

`policies.requireEgressEnforcement` refuses, at construction, a connector whose posture is not
`'enforced'`; the single-tenant preset accepts and pins the same flag. Construction also rejects an
`egressEnforcement` value outside the two literals. The Agent CLI adapters declare
`'declaration-only'`, matching their documented child-process boundary — so a deployment that sets
`policies.requireEgressEnforcement` cannot register an Agent CLI adapter, by design. Put the child
behind an infrastructure boundary, or leave the flag off for that deployment.
