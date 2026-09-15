---
'@proofoftech/breakwater': minor
---

Export `assertConnectorConformance`, a case-scoped harness a consumer runs in its own suite. For each
supplied case it builds the connector through a factory, hands it a trusted inert base transport and
an audit logger to wire, and replaces `globalThis.fetch` and any supplied transport entry point with
a trap that records the attempt and refuses. A case that reaches one fails with the named result
`NETWORK_IO_OUTSIDE_RUNTIME_FETCH`, including when the connector catches the refusal. The supplied
base transport refuses any host the registered manifest does not declare, so calling it around the
guard fails the same way.

On an absent or configurable entry point, the harness installs an accessor whose getter returns the trap. An assignment to that instrumented entry point is recorded when it happens as `INSTRUMENTATION_REPLACED` and is not applied; the trap stays in place. A writable non-configurable data property uses assignment installation, which offers no defence against assignments during execution. A redefinition still in place when the case settles or times out, or when the probe factory returns, is reported as `INSTRUMENTATION_REPLACED`. A redefinition or deletion that the case itself reverses before it settles, like a reference to `fetch` captured before the run, is outside what the harness observes. It reports the requests that pass through its trap. A detected redefinition makes the case prove `nothing` and skip expectation checks. An assignment attempt or detected redefinition during probe construction refuses the run.

`CASE_INVOCATION_FAILED`: Invocation failed before or during invocation; the reason names the error's constructor, uses `unknown` when that name is unavailable or unreadable, or describes a thrown non-Error value by type, without the message or value. During invocation, policy denials, boundary errors, and harness refusals retain their existing classifications.

Instrumentation is restored after the case settles, after a throw, after a partial install, and after
a per-case timeout; a restoration that cannot be proved fails the run and ends it, rather than
running later cases over a property the harness knows it could not put back. Entry points are
instrumented **one at a time, in order, `globalThis.fetch` first**, and every property the harness
writes is checked before it is written and verified after — both its own descriptor and the value a
caller actually reads. An accessor, an inherited accessor, a locked property descriptor, a target
that accepts a write and ignores it, and a target whose descriptor holds the replacement while the
property still resolves to the original are all refused rather than skipped — for a supplied entry
point the case is refused; for `globalThis.fetch` the whole run is when the refusal comes before any
case runs, and the case is when a case's own work makes the global uninstrumentable mid-run. Either
way the finding names the descriptor shape it found, and a failure at any entry point restores the
whole rollback stack: a (d) install or (e) verification failure includes the failing entry point; an
(a) validation or descriptor-read failure precedes capture and push, so the stack holds only the
entry points attempted before it (C8-2). A restore the target silently ignores
is reported as a failed restoration. Two entry points naming one
property is refused before any case runs, and so are two cases sharing a name, two entry points
sharing a label, and an overlapping or nested run. A factory that throws is reported as a
finding, not an opaque rejection. Each case is bounded by `timeoutMs`, 2000 ms by default; a case
that times out ends the run and no further run is accepted in that isolate, because work abandoned by
one case would otherwise be recorded against a later one. Put a test that expects a timeout last in
its file, or in a file of its own.

The harness certifies only a connector declaring `egressEnforcement: 'enforced'`. An empty case set
is reported as an empty case set, and — for a connector that declares `egress` — a set whose cases
never reach the supplied transport is reported as no transport evidence; neither is a pass, and a run
with no cases raises one finding, not both. A case whose expectations depend on policies the factory
did not wire is reported as a wiring failure naming the member, alongside the escape record, which is
kept. Every report states the finite-case limit:

> conformance covers only the supplied cases, in this isolate, for the duration of each case: it does not prove every reachable network path, a captured fetch reference, a request through an entry point the subject redefines and restores inside a case, an uninstrumented transport, another isolate, work continuing outside a case lifetime, a call the connector makes on the supplied base transport for a host the manifest already declares, or, for a connector declaring no egress, a transport the factory supplied in place of the harness's. A case that times out ends the run, because its abandoned work would otherwise be attributed to a later case, and no further run is accepted in this isolate; that abandoned work then runs against the RESTORED global, so a request it issues after the case ends is neither trapped nor recorded and leaves the process. A request that work abandoned by any settled case issues on the supplied base transport for a host the manifest does not declare, or through a trap reference it captured during that case, is recorded as a run-level finding naming that case until the report is built, and is dropped after. Entry-point targets you supply are your own test fixtures: each install is verified by its own descriptor, by an effective property read, and by the restore, against the mediations that verification names, and not against a target that adapts to those checks. Instrument globalThis.fetch alone for that guarantee.

The harness itself uses no Node built-ins, no `vm`, and no filesystem, and runs on workerd. The
barrel it ships from imports `@mastra/core/tools`, whose bundled chunks statically import Node
built-ins under unprefixed specifiers, so a Worker importing the barrel needs Node.js compatibility
enabled — the `nodejs_compat` compatibility flag, or a `compatibility_date` recent enough that your
Workers runtime turns it on by default. Check your runtime's compatibility-date documentation; in the
Wrangler/miniflare this package develops against, the default-on date is 2026-08-04.
