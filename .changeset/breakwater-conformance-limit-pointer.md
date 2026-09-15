---
'@proofoftech/breakwater': patch
---

Point the connector conformance report's `limit` at the documented channel list. The field carried a
paragraph naming the channels a run does not observe; it is now one sentence naming the permanent
URL of
[Conformance limits](https://github.com/ProofOfTechOrg/anchorage/blob/main/packages/breakwater/CONNECTORS.md#conformance-limits),
a section of the connector authoring guide that ships with the package and states each channel and
what a run records for it. A host that displays or stores `report.limit` sees the shorter text.
`CONFORMANCE_LIMIT` is not exported from the package entry points, so consumers read it only as
`report.limit`. Nothing about what the harness traps, records, snapshots or drops changes.
