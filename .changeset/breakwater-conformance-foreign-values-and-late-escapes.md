---
'@proofoftech/breakwater': minor
---

Build the connector conformance report even when a case throws a value that cannot say what it is.
The harness classified a thrown value with four separate `instanceof` reads; a value whose
`getPrototypeOf` is a trap made the classification itself throw, so the run rejected with the trap's
error and produced no report. One `classifyInvocationError` call now answers `boundary`, `policy`,
`refusal` or `foreign` once, and a value it cannot read is `foreign`: the report carries the named
`CASE_INVOCATION_FAILED` finding the contract requires. `createConnector` guards every read it makes
of the connector's own thrown value — seven guards over eight reads, and none of them in
`invokeConnector` — so that value reaches the harness intact and the execute error is still audited.

Say what each diagnostic observed, and no more. A refusal on the supplied base transport reports the
host the registered egress declaration does not cover, instead of asserting a bypass of
`runtime.fetch` that did not happen — the connector used the transport the harness gave it. A
`POLICIES_NOT_WIRED` `audit` finding for a case whose invocation failed says the invocation ended
before a witness could be recorded, instead of asserting the subject reached its gate boundary. An
`INSTRUMENTATION_REPLACED` finding for a replacement that is itself an accessor says the harness did
not check whether later calls reached the trap, instead of leaving a silence that read as though it
had. `SUBJECT_UNREGISTERED` names this copy of `createConnector()`, which is what a connector from a
second copy of the package fails against. A thrown function is described as `a function` rather than
by its source text. An escape whose address cannot be parsed — including one the URL global's
disappearance makes unparseable — is recorded with a null host instead of raising inside the
connector's own call.

Record an escape observed after its case settled. A connector that keeps the supplied base transport
or a trap reference alive past its case used to append to the case result's `escapes` array with no
finding beside it, and — once the report was built — beside a `conformant: true` the caller was
already holding. A case result now carries a snapshot of its own escapes, taken where those escapes
become findings; a later observation is a run-level `NETWORK_IO_OUTSIDE_RUNTIME_FETCH` finding whose
reason names the settled case and carries no `case`. The report snapshots `findings` and `cases` and
computes `conformant` from the snapshot, and an escape observed after that is dropped.

`CONFORMANCE_LIMIT` points at
[Conformance limits](https://github.com/ProofOfTechOrg/anchorage/blob/main/packages/breakwater/CONNECTORS.md#conformance-limits)
in the connector authoring guide, which states what a settled case's retained transport or trap
reaches, what a read of the restored global after the run closes bypasses, and what a timed-out
case's abandoned work never reaches.
