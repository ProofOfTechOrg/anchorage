---
'@proofoftech/breakwater': minor
---

Point the connector conformance report's `limit` at the documented channel list. The field carried a
paragraph naming the channels a run does not observe; it is now one sentence naming the permanent
URL of
[Conformance limits](https://github.com/ProofOfTechOrg/anchorage/blob/main/packages/breakwater/CONNECTORS.md#conformance-limits),
a section of the connector authoring guide that ships with the package and describes channels a run
observes and channels it does not. A host that displays or stores `report.limit` sees the shorter
text, and a host that asserts on its content asserts on the new sentence.
`CONFORMANCE_LIMIT` is not exported from the package entry points, so consumers read it only as
`report.limit`. Nothing about what the harness traps, records, snapshots or drops changes.
