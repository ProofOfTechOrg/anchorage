---
'@proofoftech/breakwater': patch
---

Build the connector conformance report even when a case throws a value that cannot say what it is.
The harness classified a thrown value with four separate `instanceof` reads; a value whose
`getPrototypeOf` is a trap made the classification itself throw, so the run rejected with the trap's
error and produced no report. One `classifyInvocationError` call now answers `boundary`, `policy`,
`refusal` or `foreign` once, and a value it cannot read is `foreign`: the report carries the named
`CASE_INVOCATION_FAILED` finding the contract requires. `invokeConnector` guards the three reads it
makes of the connector's own thrown value, so that value reaches the harness intact and the execute
error is still audited.

Record an escape observed after its case settled. A connector that keeps the supplied base transport
or a trap reference alive past its case used to append to the case result's `escapes` array with no
finding beside it, and — once the report was built — beside a `conformant: true` the caller was
already holding. A case result now carries a snapshot of its own escapes, taken where those escapes
become findings; a later observation is a run-level `NETWORK_IO_OUTSIDE_RUNTIME_FETCH` finding whose
reason names the settled case and carries no `case`. The report snapshots `findings` and `cases` and
computes `conformant` from the snapshot, and an escape observed after that is dropped.

`CONFORMANCE_LIMIT` points at the channel list that says so:
[Conformance limits](https://github.com/ProofOfTechOrg/anchorage/blob/main/packages/breakwater/CONNECTORS.md#conformance-limits)
in the connector authoring guide states what a settled case's retained transport or trap reaches,
what a read of the restored global after the run closes bypasses, and what a timed-out case's abandoned work does not.
