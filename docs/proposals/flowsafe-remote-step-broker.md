# Proposal: resume a durable agent after a remote step

> Status: implementation plan, not shipped. Content type: design proposal. Audience: FlowSafe maintainers and hosts that execute a remote agent tool.

This plan adds a generic durable handoff for a tool that waits for an external result. FlowSafe records the suspended run and an invocation, accepts one bound result, then resumes through its existing runtime. The host supplies its own authentication, dispatch transport, and policy; this proposal adds no customer, Workers for Platforms, signing, or capability policy to Anchorage.

## Baseline and outcome

Baseline: `plan/flowsafe-remote-step-broker` from `origin/dev` at `7d2ad352b0e4422811faf4ef4240a9172b360cce`, read 2026-10-11. Reconfirm the anchors below before editing. Use Node and pnpm versions from the root manifest. Work in a linked worktree with the shared pnpm store. The production Worker uses D1 and Durable Objects; in-memory Vitest results alone cannot prove restart behavior. The existing release is `@proofoftech/flowsafe@0.27.0` with `@mastra/core@1.73.0` as an exact peer.

Done means a remote-step tool can suspend, persist a dispatchable invocation, survive Durable Object eviction, accept a result bound to that exact suspension, and resume through `RunnerRuntime`. Duplicate dispatch and duplicate result delivery converge on the same invocation. A changed suspension or unrelated run cannot resume. Migration cutover waits for remote dispatch claims to finish or be confirmed stopped; changing the FlowSafe fence alone cannot stop an external callback already in flight. “Once” applies to accepted result and resume admission; an external side effect still needs the host's stable invocation idempotency key.

The first implementation checkpoint must prove that core 1.73.0 persists a tool's `workflow.suspend()` shape and resumes it with `workflow.resumeData` after a fresh host instance. If that probe fails, stop before publishing a broker API. A workflow-only suspension would not satisfy this agent-tool objective.

## Ownership and package choice

FlowSafe owns the durable run, suspension and resume contract. The deployment host owns authenticated result intake and the external executor. Breakwater continues to own connector authorization; Fleet Control owns deployment and migration; a customer platform owns artifact validation and grants. The generic broker never treats a caller's `runId`, `principal`, tool name, or result as authority by itself.

Package-first check, 2026-10-11:

| Candidate | Fit and decision |
| --- | --- |
| Existing FlowSafe `AgentThreadTopology`, `RunnerRuntime`, D1 storage, and execution fence | Reuse. They already provide authenticated internal Durable Object routing, exact suspension comparison, storage, and restart-aware resume. The broker needs a small state table and host adapter at this boundary. |
| Declared peer `@mastra/core@1.73.0` | Use its durable tool suspension, subject to the pinned-version probe. Do not call its direct resume API; FlowSafe's wrapper rebuilds the registry and enforces runtime admission. [Mastra durable agent guide](https://mastra.ai/docs/harness/durable-agents) and [Mastra tool suspension source guide](https://github.com/mastra-ai/mastra/blob/main/packages/core/src/tools/hitl.md) describe the general behavior, not the exact pinned implementation. |
| Cloudflare Durable Object alarms | Use the existing host's alarm and D1 patterns for retry and recovery. Alarms are at least once, so the journal and executor must tolerate repetition. [Cloudflare alarm reference](https://developers.cloudflare.com/durable-objects/api/alarms/). |
| Separate queue/workflow broker package | Reject for this seam: it cannot compare the run's authoritative suspension or execution fence without routing back through FlowSafe. It adds a second owner for state that must be correlated with the existing D1 snapshot. No new package is justified. |

No dependency or package-manager change is proposed. An adapter with a real job remains custom: it validates the broker envelope and joins D1 state to the authoritative FlowSafe run. It is narrower than another broker runtime. The source links describe current upstream behavior; repeat the pinned-core probe because current upstream code may differ from 1.73.0. License and install exposure stay at the declared FlowSafe dependency set. The candidate has no added native component or lifecycle script; the broker adds one D1 table, two bounded recovery scans, and migration/cutover obligations. Search security advisories and the lockfile again if implementation later changes dependencies.

## Contract and state transitions

Expose a host-only `RemoteStepBroker` interface from the FlowSafe agent-host subpath. The host supplies a trusted dispatch callback and a trusted result-intake adapter; FlowSafe never mounts a public result route. The unique suspension key is `{deploymentIdentity, tablePrefix, workflowId, agentId, threadId, resourceId, runId, stepPath, suspendedAt, resumeCount}`. These fields come from a storage-backed run read and verified host binding, not from the executor. `invocationId` is a separate opaque identifier generated once for the inserted key and supplied to the executor as its idempotency key. The database's unique constraint excludes `invocationId`; a concurrent insert conflict reads and returns the winning row. The deployment/table fields prevent a reused run id in another generation from claiming an earlier invocation.

Persist states `pending`, `dispatched`, `result-recorded`, `resuming`, and `settled`, with a terminal `failed` record for a permanent refusal. A D1 uniqueness constraint on the suspension key admits one invocation. A `resuming` row carries a lease owner and expiry, not permanent ownership. The result record stores a bounded, JSON-safe value or a bounded external result reference, a digest, and the exact invocation identity; it does not store arbitrary headers or credentials. Its configured retention must outlive the run's retry window and follow the deployment's purge policy. Limit size and shape at the host ingress and again in FlowSafe before D1 writes. Set the byte limit from existing D1/run bounds after the tool probe; reject oversized input rather than truncate it.

1. The tool suspends with a reserved, versioned remote-step marker and a static host-owned step key. The host uses `RunnerRuntime.authoritativeStatus()` or the thread host's storage-backed equivalent and creates the invocation only after the suspension is durable. A periodic, bounded scan of the owning deployment's `mastra_workflow_snapshot` rows selects durable agent runs suspended on that marker, then re-reads each through the authoritative host before inserting any missing row. The scan has a persistent cursor and runs even with no broker rows or armed broker alarm. An unreadable row stops that candidate without dispatch and remains visible for retry. Do not use `RunnerRuntime.status()`, which can fall back to memory, for a dispatch decision.
2. A D1 insert or read returns the same invocation for a repeated observation of the same key. The broker transactionally claims a short-lived dispatch permit against the current fence state and mutation epoch before calling the host dispatch adapter. The adapter receives the stable `invocationId` and permit generation. The executor calls the trusted host gate before its effect; that gate atomically consumes the permit once under the same deployment epoch and records its acceptance. The fence-row read, epoch comparison, and permit transition must share one D1 transaction with the fence mutation protocol; a read followed by a separate permit write is insufficient. A repeated gate call returns an `already-consumed` receipt without authorizing another effect. After a lost acknowledgement or expired permit, the broker may issue a new generation for the same invocation only after the prior claim is closed or its executor has confirmed quiescence; a new permit cannot reuse a prior epoch. The executor must deduplicate the effect by `invocationId` across permit generations. If it cannot, the host refuses remote-step dispatch.
3. Trusted result intake checks the invocation id and the complete suspension identity against the journal and current authoritative run. It inserts the first result by compare-and-swap. Identical redelivery returns the stored outcome. Different bytes or digest for the same id refuse with conflict. A result cannot select another step.
4. A recovery duty claims `result-recorded` or an expired `resuming` lease, re-reads the execution fence and `authoritativeStatus()`, and sends an internal resume with the exact `expectedSuspension`. It restores the original stored execution principal and current permissions, never the result sender's identity. It marks `settled` only after the authoritative run state proves the resume advanced or ended. If the resume response is lost, an expired lease triggers an authoritative status read: unchanged suspension retries the same recorded result; advanced or terminal state settles without replay; unreadable state retains the row for retry. A new suspension gets a new fingerprint and invocation.
5. `migration-locked` and an unreadable fence stop new permit claims and resume admission. `draining` permits completion of an existing run under FlowSafe's current fence contract. `proof-only` admits only its nominated run. Entering migration lock revokes unconsumed permits by epoch, then waits for consumed permits and in-flight executor callbacks to report settled or confirmed stopped before the migration inventory can be empty. If the executor cannot report quiescence, cutover blocks; it does not infer safety from permit expiry. Include pending and active broker rows in FlowSafe's read-only drain inventory and table census. A terminal run, canceled run, expired result, or changed fingerprint refuses the old result without starting work.

The broker deadline is the earlier of a stored run deadline, when one exists, and a required host-configured remote-step deadline. The dispatch permit's shorter expiry never extends that result deadline. At deadline or cancellation, stop issuing permits and resume retries, mark the journal terminal after an authoritative run read, and request best-effort executor cancellation. A cancel request does not prove an external effect stopped. Keep its active claim in drain inventory until the executor confirms completion or stop; otherwise cutover remains blocked. Retain the terminal journal long enough to refuse late result delivery and permit replay.

Do not claim atomicity between Mastra's snapshot write and the broker's D1 row. The periodic snapshot scan closes that gap by creating an invocation from a durable suspension; it never dispatches on a transient in-memory event. Do not claim exactly-once external execution without an idempotent executor. A lost resume acknowledgement leaves `result-recorded` or `resuming` for a later authoritative status read.

### Risky edit: resume authority

Current `AgentThreadTopology.resume` accepts an `ApprovalRecord` and `ApprovalDecision`, then sends `defaultResumeData(record, decision)` with `expectedSuspensionFor(record)`. The target host reauthorizes the stored principal and calls `durable.resumeViaRuntime()`. A remote result is not an approval and must not fabricate either record. Add a distinct host-only result-resume method whose request carries the journaled invocation and result. It must reuse the target host's run ownership, catalog authorization, `withExecution`, and `resumeViaRuntime` path, with mandatory `expectedSuspension`. Do not broaden the existing public agent router or optionalize the fingerprint for this method.

Before (`thread-topology.ts`, `AgentThreadTopology.resume`):

```typescript
resume(
  context: ActorContext,
  record: ApprovalRecord,
  decision: ApprovalDecision,
): Promise<AgentRunEnvelope>;
```

After, as a separate interface rather than changing approval semantics:

```typescript
resumeRemoteStep(
  context: ActorContext,
  invocation: StoredRemoteStepInvocation,
  result: RecordedRemoteStepResult,
): Promise<AgentRunEnvelope>;
```

The implementation must not accept an arbitrary caller-constructed `StoredRemoteStepInvocation`; the topology loads or validates its D1 row by id before sending. The thread host independently reads the same row and authoritative snapshot before execution. If adding a separate entry path to `AGENT_ENTRY_PATHS`, treat it as a continuation like `approval.resume`: a principal kind that could start the run may resume unless it has been removed from the catalog. Never classify the result sender as the run principal.

## Implementation checkpoints

### 1. Pinned-core suspension probe

Use a deterministic model and a tool with `suspendSchema`/`resumeSchema`. Assert that the tool's `workflow.suspend()` reaches `RunSummary` with a distinguishable payload, survives a new FlowSafe host instance on persisted storage, and receives only the recorded result after `resumeViaRuntime`. Exercise refusal for malformed resume data and a second suspension. Add this to the existing real-loop agent tests; do not add a production export for the test. If the pinned core cannot safely resume that tool, document the failure and choose a supported workflow step boundary in a new reviewed plan.

### 2. Journal and dispatch admission

Add the D1 broker table through the existing FlowSafe provisioning/migration path. Use a versioned canonical envelope, uniqueness on the suspension key excluding `invocationId`, CAS result writes, bounded payloads, and a stable invocation id. Add the row to the inventory work category and schema census. Reuse the current D1 error and transaction patterns; no new parser or cryptographic primitive. Reconcile a suspension after a crash before insertion from the periodic snapshot scan, including when no broker row exists. Recover dispatch after acknowledgement loss through the permit protocol. Dispatch requires an authoritative status and a transactionally claimed permit at the current fence epoch. The executor contract requires idempotency by `invocationId` and an authenticated acknowledgement of quiescence.

### 3. Result and runtime continuation

Add a host-only topology method and target route. Validate deployment identity, internal request authentication, owner, thread/resource/run correlation, journal identity, and current suspension before `resumeViaRuntime`. Restore the original principal and current catalog policy. Bind the result as resume data to the exact tool step. Preserve the existing approval resume route. Record a durable outcome after reading authoritative status, including when the resume response is lost.

### 4. Recovery and public documentation

Connect two bounded scans to the existing maintenance alarm pattern: the periodic snapshot scan finds orphaned remote suspensions, and the broker-row scan recovers `pending`, `dispatched`, `result-recorded`, and expired `resuming` leases. Neither depends on an existing broker row to schedule its successor. Recover from isolate loss; do not retry a permanent refusal as execution. A timeout or cancellation requests best-effort executor cancel and keeps active claims visible until confirmed quiescent. Document the generic host contract, permit generations and epoch revocation, the requirement for external idempotency, fence/cutover behavior, retention, and the absence of a public result route. Add a FlowSafe minor changeset for the new API after behavior passes the real-worker proof.

## Reuse map and exclusions

| Existing code | Reuse |
| --- | --- |
| `packages/flowsafe/src/agent-host/thread-topology.ts`, `createAgentThreadTopology` | Authenticated internal routing and owner-checked status |
| `packages/flowsafe/src/agent-host/thread-host.ts`, `createAgentThreadHost` | Stored principal, current catalog permission check, serialized execution |
| `packages/flowsafe/src/agent-runner/durable-agent-runner.ts`, `resumeViaRuntime` | Fresh-host registry rebuild and runtime resume |
| `packages/flowsafe/src/do-runner/runtime.ts`, `RunnerRuntime.authoritativeStatus` and `resume` | Storage-backed state, fence and exact suspension check |
| `packages/flowsafe/src/do-runner/execution-fence.ts` | Deployment admission and migration lock |
| `packages/flowsafe/src/do-runner/d1-storage.ts`, deadline snapshot scan | Bounded, persisted-cursor D1 scan pattern for orphan discovery |
| `packages/flowsafe/src/do-runner/inventory.ts` and `mastra-schema-guard.test.ts` | Read-only drain classification and table census |
| `packages/flowsafe/src/agent-runner/agent-gate-round-trip.test.ts` | Real runtime suspension and grant round-trip test pattern |

Out of scope: WFP source or build admission, artifact signatures, capability grants, user/org quotas, customer billing, and public authentication. Those are platform policy. Approval decisions remain approval decisions; the broker does not mint Breakwater approval grants from a remote result. The durable-result confidentiality proposal is separate: this broker must not claim its stored result is output-policy approved, and hosts should avoid sensitive result content until that gate is implemented.

## Verification and release gate

Run each changed test file once per checkpoint, then the owning package checks allowed by the repository quality gate. Send complete output to `/tmp` logs and record exit status. Candidate commands for the implementation worktree:

```bash
pnpm --filter @proofoftech/flowsafe exec vitest run \
  src/agent-runner/durable-agent-runner.test.ts \
  src/agent-runner/agent-gate-round-trip.test.ts \
  src/agent-host/thread-topology.test.ts \
  src/agent-host/thread-host.test.ts
pnpm --filter @proofoftech/flowsafe typecheck
pnpm --filter @proofoftech/flowsafe build
pnpm --filter @proofoftech/flowsafe spike:verify
```

For the shared protocol and migration checkpoint, run the owning package's whole test suite once because the exported contract and D1 schema have broad consumers. Verify a real D1/workerd path with a fresh isolate and duplicate callback. After all checkpoints, run the repository's full suite once under the quality-gate rule, including the direct scenario filters that exercise changed contracts. Require these observations: one persisted invocation per suspension key under concurrent observations; orphan suspension discovery with no broker row; no dispatch before durable suspension; permit replay/renewal across an epoch change; identical result replay; conflicting result refusal; stale or foreign result refusal; cutover blocked while a callback remains active; deadline and cancellation stop retries but do not falsely claim executor stop; successful original-principal resume after eviction; recovery of an expired `resuming` lease; no repeated external side effect with a conforming idempotent executor; one terminal run state after lost acknowledgements.

The implementation is auth, migration, data-retention, and concurrency work. Each code checkpoint is T2: clean-code, QA, architecture, and security lanes. This plan gets one design-level review when added; reviewers do not simulate its commands. A change to this plan after implementation starts records the departure once at completion. Merge a reviewed feature branch into `dev`, not `main`; publish only through Anchorage's version workflow.

## Appendix: current-state anchors

These are verbatim at the baseline commit. Reconfirm them before applying the plan.

### Thread status and resume (`packages/flowsafe/src/agent-host/thread-topology.ts`)

```typescript
status(
  context: ActorContext,
  input: AgentThreadRunRef,
): Promise<AgentRunEnvelope | undefined>;
```

`createAgentThreadTopology` routes status through `threads.send()` after `requireResourceAccess` on thread, resource, and run. Its approval resume currently sends:

```typescript
resumeData: defaultResumeData(record, decision),
...expectedSuspensionFor(record),
entryPath: 'approval.resume',
requestedBy: record.decidedBy,
```

### Current suspension correlation (`packages/flowsafe/src/agent-host/thread-host.ts`, `currentApprovals`)

```typescript
runRecord.suspendedAt === summary.suspendedAt?.[key] &&
runRecord.resumeCount === summary.resumeCount?.[key]
```

`thread-host.ts`'s resume route calls `authorize()` on the stored principal and then:

```typescript
return durable.resumeViaRuntime({
  runId: ref.runId,
  requestedBy: requesterId,
  ...(step !== undefined ? { step } : {}),
  ...resumeFields,
```

### Runtime check (`packages/flowsafe/src/do-runner/runtime.ts`, `RunnerRuntime.resume`)

```typescript
const proof = await this.#assertResumeFence(workflowId, runId);
return this.#withRunLock(workflowId, runId, async () => {
```

Later in the same resume, the exact suspension check is:

```typescript
if (
  leg.suspendedAt !== expectedSuspension.suspendedAt ||
  leg.resumeCount !== expectedSuspension.resumeCount
)
  throw new SuspensionChangedError(workflowId, runId, stepKey);
```

### Guarded entry grammar (`packages/flowsafe/src/agent-runner/agent-run-state.ts`)

```typescript
export const AGENT_ENTRY_PATHS = [
  'http.start',
  'approval.resume',
  'signal.message',
```

The array continues with other existing entry paths. `packages/flowsafe/src/agent-host/catalog.ts` treats `approval.resume` as a continuation for automated principals. A new broker continuation must preserve that distinction.
