---
content_type: Conceptual
goal: Select one durability authority for Super platform runs
audience: Flowsafe maintainers and Super platform runtime engineers
content_plan: Define the benchmark, report runtime evidence, compare the authorities, record the decision and its limits
---

# Which runtime owns durable runs?

FlowSafe’s Durable Object runner owns the Super platform run protocol. Cloudflare Workflows remains a benchmark-only implementation. Production code must not start or resume both authorities for one logical run.

## Benchmark baseline and method

The benchmark hand-maps one approval-gated graph onto both runtimes: provider delivery, research, approval wait, stored grant reconstruction, and one externally visible publish effect. It uses the existing Mastra graph on FlowSafe and equivalent explicit steps on `WorkflowEntrypoint`; it does not add a graph compiler.

The evidence in this file was collected on 2026-08-10 from branch `dev` at base commit `8071fac15663c1338d3c5bdc0aab9f1fe0edd21e`. The local runtime used Wrangler 4.107.0, `@cloudflare/workers-types` 4.20260702.1, and `@mastra/core` 1.50.0.

Run the benchmark from `packages/flowsafe`:

```bash
node scripts/durability-benchmark.mjs
```

The command creates an isolated temporary Wrangler state directory, seeds the deployment sentinel in local D1, starts workerd, and kills the entire process group at two failure boundaries. It restarts workerd against the same persisted state and deletes the temporary directory after the run.

## Observed comparison

Both implementations complete the representative graph and recover an approval wait after process loss. Their authority surfaces differ:

| Criterion | FlowSafe Durable Object runner | Cloudflare WorkflowEntrypoint |
| --- | --- | --- |
| Snapshot and resume fidelity | D1 status exposes the Mastra result, exact suspended step path, payload, suspension timestamp, resume ordinal, and execution principal | The binding exposes coarse instance status and terminal output; the local runtime also exposes development-only step outputs |
| Grant reconstruction | Existing `approvalGrantProvider()` derives an exact suspension-scoped grant from the approved D1 record; the real Breakwater connector accepts it | The hand-mapped step needs a benchmark-specific D1 approval query and grant object |
| Retry semantics | The run Durable Object serializes start and resume; Mastra persists step boundaries; effects still require an idempotency key | `step.do()` supports explicit retry count, delay, backoff, and timeout; step results are cached by the platform |
| Process-loss recovery | The selected matrix proves suspended and terminal recovery across killed workerd generations | The comparison proves a waiting instance resumes after the same workerd kill |
| Inspectability | Run and approval state are queryable in the deployment’s D1 database with exact approval provenance | Public binding status is coarse; platform step history is a separate operational surface |
| Authority-specific core code | 137 physical source lines in the benchmark worker | 45 physical source lines in the benchmark worker |
| Benchmark HTTP adapter code | 139 physical source lines | 105 physical source lines |

Cloudflare Workflows has the smaller executor and stronger built-in step retry controls. Those advantages do not outweigh the second approval model and the loss of FlowSafe’s exact Mastra suspension and grant protocol. Interoperation would require one runtime to become a transport adapter under the other; running them as peers would create two run IDs and two completion answers.

## Selected-protocol failure matrix

The FlowSafe matrix passes against real local Wrangler, workerd, Durable Objects, and D1:

| Failure or race | Assertion |
| --- | --- |
| Termination before approval | The exact suspended snapshot and approval record survive a process-group kill and restart |
| Termination after approval | The terminal result and single effect survive a second process-group kill and restart |
| Duplicate resume | A resume after terminal success returns a conflict and does not execute the connector |
| Duplicate approval decision | The D1 compare-and-swap transition accepts one decision and rejects the replay |
| Duplicate provider delivery | The delivery ledger returns the original run ID and does not start the caller-supplied alternate run |
| Cross-run Durable Object ID | A Durable Object addressed for one run rejects a request naming another run |
| Cross-run approval ID | An approval from a completed run cannot resume a different suspended run |
| Store contention | Two concurrent reviewer decisions produce one success, one conflict, and one effect |
| Effect at most once | The D1 effect key remains unique after duplicate delivery, decision, resume, and contention probes |

The publish effect uses an explicit `(authority, run_id, effect_key)` uniqueness key. Selecting a durability authority does not make non-transactional provider side effects exactly once; connector implementations must preserve their provider idempotency keys.

## Production boundary

The Cloudflare Workflow binding, entrypoint, routes, and D1 approval adapter exist only in `spike/durability-benchmark.worker.ts` under `spike/durability-benchmark.wrangler.jsonc`. They are absent from package exports, deploy handlers, and consumer templates. FlowSafe remains the only production orchestration authority.

Do not add a production `WorkflowEntrypoint` path beside `DurableObjectRunner`. Revisit this decision only with a replacement design that preserves exact suspension-bound grants, one canonical run ID, and one canonical completion state.

## Evidence limits

This benchmark establishes protocol behavior under local workerd process loss and real local D1 contention. It does not prove Cloudflare control-plane retention, regional failover, account-level Workflow limits, or production dashboard behavior. Those platform properties require a credentialed Cloudflare account and do not change the one-authority decision.
