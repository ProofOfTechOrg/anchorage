---
'@proofoftech/flowsafe': minor
---

Track A (durable agents): drive Mastra's durable-agent loop through the one
RunnerRuntime chokepoint so agent legs inherit the substrate's invariants —
additive and opt-in, no existing signature or `ApprovalRecord` shape changed.

- `@proofoftech/flowsafe/agent-runner` (new subpath): `createFlowsafeDurableAgent`
  returns a `DurableAgent` subclass whose `executeWorkflow(runId, workflowInput)`
  calls `runtime.start('durable-agentic-loop', { runId, inputData })` instead of
  the base `createRun + start` (DL-001/DL-010), so INV-1 (server-minted runId),
  the per-leg `requestContextForRun` grant derivation, and the resume ledger
  apply to agent legs. `stream()`/`generate()`/`prepare()` are overridden to
  REQUIRE a caller-minted runId — closing every inherited minting entry point's
  upstream `crypto.randomUUID()` fallback (INV-1: a durable-agent run must carry
  its tenant everywhere it becomes a key). The shared loop workflow is registered on
  the runtime idempotently; the agent's stream pubsub defaults to the runtime's
  identity (one feed per DO). `resume()` stays non-client-facing — resume flows
  only through the approval-decision path (grant-only doctrine, P8).
- The durable tool-call step hands `tool.execute` the ENGINE-LEG requestContext
  from its step params (spike S1, verified against `@mastra/core` 1.50.0 dist),
  so the `breakwater.approvedConnectors` grant reaches the connector write gate
  with zero extra wiring and a forged/self resume fails closed there.
- R-003: the record-creation/bridge path parses BOTH durable approval-suspend
  shapes — nested `{ type:'approval', requireToolApproval:{ toolCallId, toolName,
  args } }` and flat `{ type:'approval', toolCallId, toolName, args }` — and
  derives `connectors:[toolName]` (the connector id the write gate checks) so an
  approved agent gate mints exactly that grant. The resume-routing
  `threadId`-capture seam (DL-002) is deferred to Track C, where the thread-DO
  consumes it.
- `RunnerRuntime` now threads the host pubsub identity into both `createRun`
  sites (`createRun({ runId, pubsub })`); undefined leaves behavior
  byte-identical (polling fallback).
- The workerd `spike:verify` grows agent-gate scenarios proving the R-003
  round-trip and forged-resume fail-closed on real workerd + D1.
