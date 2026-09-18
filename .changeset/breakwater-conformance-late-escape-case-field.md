---
'@proofoftech/breakwater': minor
---

Name the settled case on a late conformance finding as a field, not only in prose.
`ConnectorConformanceFinding` gains an optional `observedAfterCase`, which carries the name of the
case whose abandoned work produced a run-level finding — an escape or a finding that arrived after
that case settled. Such a finding still carries no `case`, because the case's own result is already
on the report, and its `reason` is unchanged, so a host that reads the sentence keeps reading it. A
finding the probe phase produces carries no `observedAfterCase`: no case had settled.
