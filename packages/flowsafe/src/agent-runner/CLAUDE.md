# agent-runner/

Track A (DL-001/DL-010): drive Mastra's durable-agent loop through the ONE
`RunnerRuntime` chokepoint so agent legs inherit INV-1, the per-leg
`requestContextForRun` grant derivation, and the resume ledger. Subpath-only
export `@proofoftech/flowsafe/agent-runner` (like host-kit) — it imports the
durable `Agent`, which drags `@mastra`'s Node built-ins, so it stays out of the
root barrel and off the browser's import graph.

## Files

| File | What | When to read |
| ---- | ---- | ------------ |
| `durable-agent-runner.ts` | `createFlowsafeDurableAgent({ agent, runtime, cache?, pubsub?, maxSteps? })` → a `DurableAgent` subclass whose `executeWorkflow(runId, workflowInput)` drives `runtime.start('durable-agentic-loop', { runId, inputData })` instead of the base `createRun + start`. INV-1 is enforced at the PUBLIC boundary: `stream()`/`generate()`/`prepare()` are overridden to REQUIRE a caller-minted runId (one shared `#assertCallerRunId` guard) — the inherited entry points take an optional runId and core would otherwise mint a tenant-less `crypto.randomUUID()` upstream of `executeWorkflow`'s own (defense-in-depth) guard (`prepare()` also registers the run under that id, so it too must guard). The factory registers the shared loop workflow on the runtime idempotently (every durable agent compiles to the one `durable-agentic-loop` id; `agentId` in the input routes to the right agent); the agent's stream pubsub defaults to `runtime.pubsub` (one feed per DO, DL-001). `resume()` is inherited but NEVER client-wired — resume flows only through the approval-decision path (P8/A-D2). `DURABLE_AGENTIC_LOOP_WORKFLOW_ID`, plus `RUNTIME_DRIVEN_AGENT`/`isRuntimeDrivenAgent` — the brand FlowsafeDurableAgent carries so Track C's thread-DO signal WAKE (which starts a run) can require a runtime-driven agent and refuse a plain one fail-closed (a plain Agent's wake runs the loop OFF the runtime) | Wrapping an agent for durable, runtime-driven execution; changing the drive/registration |
| `approval-shapes.ts` | Pure, dependency-free R-003 parsing of the durable approval-suspend payload (BOTH shapes: flat `{type:'approval',toolCallId,toolName,args}` and nested `{type:'approval',requireToolApproval:{…}}`). `parseAgentApprovalSuspend`, `agentGateConnectors` (→ `[toolName]`, the connector id the write gate checks, so an approved agent gate mints exactly that grant — consumed by host-kit's `requestedConnectors`). No `@mastra` import, so the bridge can call it without dragging the durable Agent into a browser-reachable module. (The resume-routing `threadId`-capture seam, DL-002, is deferred to Track C.) | Changing how the agent suspend shapes are parsed or how the connector-to-grant is derived |
| `index.ts` | Subpath barrel | Finding the agent-runner export surface |
| `durable-agent-runner.test.ts` | Drive (`runtime.start` with the right args), INV-1 guard (no crypto fallback), idempotent registration — with a mock model (the loop drive never invokes the LLM) | Changing the runner's drive/guard/registration |
| `approval-shapes.test.ts` | Both suspend shapes parse; `agentGateConnectors` derivation and fail-closed cases | Changing the shape parsing |
| `agent-gate-round-trip.test.ts` | The S1/S2/R-003 composition against the REAL runtime + breakwater connector + grant provider + host-kit bridge: an approved agent gate (both shapes) mints the grant the connector demands and the run completes; a forged resume fails closed; a self-decision is denied (SoD) — the durable loop's tool-call gate mechanic made executable without an LLM | Verifying the engine-leg-context → connector → grant round-trip |

## Scope note (Track A)

Agent loops run PER-RUN in Track A: resume routes by (workflowId, runId,
stepPath) through `RunnerRuntime.resume`, so no `threadId` is needed — capturing
it for thread-affine routing is deferred to Track C (the thread-DO). Within one
DO isolate the loop resolves the tool's execute closure from the in-process
`globalRunRegistry` (populated by `stream()`); a resume AFTER DO eviction must
first rehydrate that registry with `DurableAgent.prepare()` (snapshot's
`messageListState` wins) before `runtime.resume` — the S3 seam, owned by the host
that wires the resume topology.
