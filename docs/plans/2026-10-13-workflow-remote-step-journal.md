# Workflow-target remote-step journal

> Content type: implementation plan. Audience: FlowSafe maintainers. Status: planned; customer dispatch remains disabled.

## Baseline and environment

- Branch `plan/flowsafe-remote-step-broker` at `ee4003a42882c0d1282004d5fed0274a5b4e2939` (2026-10-13). Reconfirm the appendix anchors against the checked-out commit before editing. The concurrent state-egress checkpoint owns `src/host-kit/state-egress.ts` and `flowsafe-worker.ts`; this plan does not edit them.
- Node and pnpm come from the root manifest. Work in the linked worktree with the shared package store. Capture checks in `/tmp` with a final `EXIT_STATUS` line. D1 `batch()` is transactional according to [Cloudflare's API documentation](https://developers.cloudflare.com/d1/worker-api/d1-database/#batch).
- This is a default-off journal contract. No workflow host route, ASP permit authority, customer dispatch, or automatic `run.resume` is enabled by this checkpoint.

## Decision

Extend the existing `RemoteStepJournal` D1 authority to distinguish `agent` and `workflow` target rows. Preserve every v3 agent row and its permit/result state. A workflow row uses the trusted start provenance `{kind:'workflow', id: workflowId}` and has no agent, thread, or resource ID. Its suspension carries a versioned marker and exact JSON request bytes; the journal stores a SHA-256 digest of those bytes. Registration and permit consumption compare the same authoritative snapshot, principal, deployment, start token, fence generation, suspension identity, request bytes, and deadline as the agent path. The first retained result remains immutable under the consumed permit digest.

The workflow suspension wire shape is `suspendPayload[stepPath.join('.')].remoteStep = { marker: 'flowsafe.remote-step.workflow/v1', request: <JSON value> }`, alongside `type: 'suspension'`. The trusted workflow host must emit this shape; user code cannot obtain authority merely by constructing the payload. The marker is a versioned parser selector, not proof of ownership. Confirm it against the ASP v2 author before wiring a caller.

Reject synthetic agent/thread/resource IDs: they would make a workflow row appear to be an agent row and let agent-host routing or cleanup misinterpret its authority. Reject a second workflow permit table: two authorities would need a new cross-table uniqueness and replay protocol. Reject a preflight-only public API: it could be mistaken for dispatch authority while storing no permit or result. Keep general Mastra `run.resume` and automatic recovery fail-closed: the pinned `@mastra/core@1.73.0` does not fence all snapshot write paths by remote attempt generation (see [durable-resume plan](2026-10-12-remote-step-durable-resume-recovery.md)).

## Reuse and package decision

Reuse `RemoteStepJournal`, `ExecutionFenceStore`, `assertExecutionPrincipal`, `isPathSafeId`, authoritative runtime status/start state, D1 conditional updates, and Web Crypto SHA-256 in `src/do-runner/remote-step-journal.ts`. The existing direct dependencies are `@mastra/core@1.73.0`, `@mastra/cloudflare-d1@1.4.0`, and `jose@6.2.8`; none provides this host-specific journal schema and CAS contract. A new package adds no useful boundary. A separate custom store duplicates the existing permit protocol. The narrow custom change is a discriminated target row and suspension selector in the existing journal. No dependency changes.

## Checkpoints

1. **Schema v4.** Rebuild the journal table transactionally because SQLite cannot remove `NOT NULL` from agent identity columns in place. Add `target_kind`, `suspension_marker`, `request_digest`, and `write_revision`; make agent/thread/resource nullable. Copy v3 rows as `agent`, with their original marker, `NULL` request digest, and revision zero; preserve their status, permit, result, lease, and settlement fields. The agent branch continues to compare the exact stored `request_json`; it does not require a digest on migrated rows. A workflow row requires a digest. Add separate partial unique indexes for agent and workflow suspension identities, plus the recovery index. Validate exact schema/index/trigger definitions and refuse unknown versions. A v4 update increments `write_revision` in the same statement as its state transition. A D1 trigger rejects updates that do not advance it; v3 code omits that column, so an old in-flight isolate cannot mutate a row after migration. `target_kind NOT NULL` rejects its inserts. Test a v3 initialized journal instance attempting both operations after another instance migrates. A v4 binary reads migrated agent rows unchanged. Keep rollback to a v4-aware binary; an old v3 binary fails closed.
2. **Workflow storage port.** Add a workflow-specific registration method, a workflow getter, and typed workflow invocation. Registration accepts a trusted workflow address and original principal, verifies start provenance and exact suspension, then performs a snapshot-guarded insert/update. Existing agent-host `get` and `register` remain agent-only. Shared permit, consume, result and retained-result methods dispatch by target kind inside the same journal authority. No caller is wired yet.
3. **Proof.** Use real SQLite-backed D1 semantics for old-row preservation, conflicting registration, restart/reopen, permit replay, wrong request/principal/start token/fence/deadline, and result conflict. Add a workerd fresh-isolate scenario before claiming a live persistence proof. The default-off package may merge without a customer route; it may not be described as a working WFP dispatch path until the authenticated host route and workerd proof land.

## Verification

After each code checkpoint, derive scoped checks from changed paths. The journal schema is a shared contract, so the owning package's full test suite applies once under the quality-gate exception. Candidate commands:

```bash
pnpm --filter @proofoftech/flowsafe exec vitest run src/do-runner/remote-step-journal.test.ts
pnpm exec biome check packages/flowsafe/src/do-runner/remote-step-journal.ts packages/flowsafe/src/do-runner/remote-step-journal.test.ts
pnpm --filter @proofoftech/flowsafe typecheck:test
pnpm --filter @proofoftech/flowsafe build
pnpm --filter @proofoftech/flowsafe test
```

Each command needs its own `/tmp` log and actual exit status. Done for this checkpoint means schema upgrade preserves v3 rows, workflow register/permit/result behavior passes, agent-host behavior passes, and no public workflow dispatch or resume route is enabled. The top-level quality-gate owner runs T2 clean-code, QA, architecture, and security lanes, then commits. Live enablement additionally requires a real D1/workerd restart and replay proof, authenticated host binding, and ASP grant verification.

## Appendix: current-state anchors

At the baseline SHA, `src/do-runner/remote-step-journal.ts` declares an agent-only address and row:

```ts
export interface RemoteStepAddress {
  deploymentId: string;
  tablePrefix: string;
  workflowId: string;
  agentId: string;
  threadId: string;
  resourceId: string;
  runId: string;
}
```

The `RemoteStepJournal.initialize` table has `agent_id TEXT NOT NULL`, `thread_id TEXT NOT NULL`, and `resource_id TEXT NOT NULL`, with a unique key containing those columns. Its `readSchema` accepts version 3 or 2 and exact column/index definitions. Before → after: v3 agent-only fields and uniqueness → v4 discriminated target with nullable workflow agent fields and partial target-specific unique indexes. A simple nullable-column alteration is insufficient because the existing unique key cannot represent workflow identity.

`RemoteStepJournal.#execution` currently refuses workflow starts:

```ts
if (
  state?.storage !== 'd1' ||
  target?.kind !== 'agent' ||
  !isPathSafeId(target.resourceId)
)
  throw new RemoteStepRefusal(
    503,
    'remote step run identity is unreadable',
  );
```

Before → after: the agent branch retains its exact checks; the workflow branch requires `target.kind === 'workflow'`, `target.id === address.workflowId`, no agent-start provenance, matching original principal, deployment/table prefix/run ID/start token, and the current execution fence. It never fills agent fields from workflow IDs.

`selectedSuspension` currently matches `toolCallSuspended.remoteStep === REMOTE_STEP_MARKER` and reads `tool.request`. Before → after: keep that agent selector and add a separate workflow selector for the versioned `remoteStep.marker` and `remoteStep.request`, with one selected path and exact request bytes/digest.

`RemoteStepJournal.issuePermit` and `consumePermit` compare `suspension.requestJson` against `JSON.stringify(invocation.request)` and conditionally update the journal with `request_json = ?`, snapshot equality, and `executionFenceAdmissionSql`. Before → after: retain those CAS predicates and add the target kind and marker. The workflow branch also compares its stored request digest; legacy agent rows continue to use exact request bytes without a digest. Every v4 state update advances `write_revision` under a D1 trigger, which rejects a previously initialized v3 isolate's updates. `recordResult` keeps `permit_digest` and immutable result comparison. No code path retries `run.resume` from a retained result.

`src/do-runner/execution-admission.ts` defines `StartIdentity.target` as a union with `{kind:'workflow',id}`. `src/do-runner/runtime.ts` already checks that a recovered workflow target ID equals its execution workflow ID. The new journal branch uses that trusted provenance rather than creating another identity format.
