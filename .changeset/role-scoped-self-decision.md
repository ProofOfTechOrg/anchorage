---
"@proofoftech/flowsafe": minor
---

Add a role-scoped separation-of-duties exemption. `ApprovalService`'s
`allowSelfDecision` option now accepts `boolean | { roles }` — `true` exempts
every decider, `{ roles }` exempts only the listed roles (a single-operator
deployment sets e.g. `{ roles: ['admin'] }`). Composed hosts reach it through
the new `APPROVAL_ALLOW_SELF_DECISION` env var (a `false` spelling, a CSV of
roles, or `true`; any invalid value falls back to OFF — SoD stays on).
A permitted self-decision is audited with `detail.selfDecision: true`, and the
run catalog echoes `actor.canSelfDecide` so a UI can drop its "the server will
refuse your decision" hint for an exempt role. Default behavior is unchanged
(SoD on).
