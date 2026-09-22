# Deferred work

Work that was designed and approved but set aside, with what it would deliver and the constraints it must still meet, so it can be picked up without redoing the design.

## Live-run cost evidence for the direct credentialed conformance lane

Deferred on 2026-09-22 so that the lane's live proof, audit reconciliation and release could proceed first.

What it delivers: a `cost` record in the direct credentialed conformance evidence that states, for the single resource-creating live run, per-resource usage observations — Worker requests and CPU time, Durable Object duration and active/CPU time, D1/SQLite rows read and written, storage bytes — for every resource incarnation the run creates (the reference control plane and every tenant deployment alike), captured before the destructive step deletes each one and reconciled afterwards by a read-only `--cost-report` mode; plus the account's billable-usage rows for the charge periods the run spans. It replaces the evidence's overbroad `basis: 'request-counters'` with a statement of which transports are counted and which are not (the bootstrap's direct provider calls are the named gap) and removes the overloaded `billed: null`, with a minor `@proofoftech/fleet-control` changeset.

What it does not deliver: a per-run monetary total. The billable-usage API has no run or resource dimension and an account carries earlier consumption, so `RunCostAttribution` is fixed at `not-attributable`; a per-tenant cost in money is a multiplication against the price list outside the tool.

Constraints that hold when it is picked up: a private, atomic, run-bound cost baseline is persisted before any bootstrap mutation and reused on resume; cost state stays out of the run journal and its byte budget; no second HTTP client — the cost session reuses the provider transport's fetch, origin, redirect, deadline and byte protections; no arbitrary provider JSON spread into evidence, no invoice or profile data, no secret keys in any artifact; a billing or analytics failure never blocks cleanup, teardown, normal evidence or `handle.close`; no data-query arm for Durable Object SQL storage until that dataset's fields, filter and units are verified; the explicit date-only billable-usage interval is unprobed until a read-only probe establishes it.
