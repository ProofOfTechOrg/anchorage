# Workflow-target remote-step journal

> Content type: implementation plan. Audience: FlowSafe maintainers. Status: journal and private dispatch implemented in the 0.29.0 source; the [continuation supplement](#continuation-supplement-2026-10-16) is planned. Customer dispatch remains disabled.

This plan records the workflow journal design and its additive continuation work. The original sections describe the 2026-10-13 checkpoint. The continuation supplement supersedes that checkpoint’s exclusion of normal workflow continuation; automatic Mastra replay remains excluded.

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

## Continuation supplement, 2026-10-16

Resume a retained workflow result once through its owning Durable Object. ASP can register a fixed trusted customer workflow in its normal runtime; this change does not require a dynamic workflow catalog or customer code inside the state Worker.

### Baseline and execution environment

Use branch `feat/workflow-remote-step-continuation` at `91db19efe32cc87ef669eb7350198d10a0511cba` in `/home/gcharang/gitrepos/proofoftech/anchorage-workflow-remote-resume-20261016`. Its tree is the verified 0.29.0 source. Reconfirm the [continuation anchors](#appendix-continuation-anchors) before editing; line numbers refer to this baseline.

Use Node 22.22.3 and pnpm 10.34.4 through `fnm exec --using 22.22.3 --`. The frozen install passed in `/tmp/flowsafe-workflow-continuation-install-20261016.log`. Keep verification output and build scratch under `/tmp`, retain each command’s exit status, and use the shared package store. The separate 0.29.0 release worktree is outside this checkpoint.

### Decision and reuse boundary

Add workflow-specific claim and settlement methods to `RemoteStepJournal`, an opt-in `DurableObjectRunner` hook, and an authenticated continuation operation on the existing private workflow route. Keep agent getters and continuation methods agent-only. The existing v5 table stores the workflow identity, result, lease and settlement; no migration is needed.

Reuse these repository boundaries:

- `src/do-runner/remote-step-journal.ts`: workflow registration, retained-result validation, `#stableSnapshot`, `#execution`, `#admission`, conditional writes and write counters
- `src/do-runner/durable-object.ts`: deployment request verification, object identity, trusted principal decoding, `runOwnership`, `#withOperationLock`, `#withLeg` and resume boundary reconciliation
- `src/do-runner/runtime.ts`: authoritative start state, existing-run fence admission, `resume` and expected-suspension validation
- `src/host-kit/do-run-topology.ts`: run addressing and authenticated Worker-to-object headers
- `src/host-kit/flowsafe-worker.ts`: private dispatch authorization and actor context derived from stored authority
- `src/agent-host/workflow-remote-step-route.ts`: bounded exact-body operations under the private route
- `src/do-runner/suspension-deadline.ts`: nested-workflow metadata identification and the reserved resume-envelope pattern

Package decision checked 2026-10-16: the declared `@mastra/core@1.73.0` peer, `@mastra/cloudflare-d1@1.4.0` and `jose@6.2.8` remain unchanged. This extends FlowSafe’s own persisted protocol. A third-party orchestration library would introduce another execution authority without implementing the journal’s existing predicates. Reusing those predicates is narrower than another store or package. No dependency, cryptographic primitive, credential format or authentication protocol changes.

The earlier exclusion of workflow-target continuation in this plan and the [durable-resume plan](2026-10-12-remote-step-durable-resume-recovery.md) is superseded for first admission through the owning RunDO. Their prohibition on automatic replay still applies. A persisted claim, expired lease, unchanged suspension or replacement isolate never authorizes another engine call.

### Additive journal and host contracts

Define the workflow continuation reader beside the journal’s existing exported types. Its methods select workflow rows; do not widen the existing agent methods.

```ts
claimWorkflowResume(
  invocationId: string,
  leaseMs: number,
  expectedMutationEpoch?: number,
): Promise<string>;

settleWorkflowAfterResume(
  invocationId: string,
  leaseOwner: string,
  witnessed: RunSummary,
): Promise<'applied' | 'terminal-not-success' | 'unresolved'>;
```

`WorkflowRemoteStepContinuationReader` selects `deploymentId`, `tablePrefix`, `getWorkflow`, `readRetainedResult` and those methods from `RemoteStepJournal`. A claim validates `expectedMutationEpoch` against the observation returned by `#admission`; its SQL uses that observation’s predicate. An absent epoch is accepted only when the existing fence contract permits it. Bound the requested lease using the existing 60,000 ms maximum. Expiration does not permit reclaim.

Add this protected hook to `DurableObjectRunner`, returning `undefined` by default:

```ts
protected async workflowRemoteSteps(
  env: TEnv,
  runtime: RunnerRuntime,
): Promise<WorkflowRemoteStepContinuationReader | undefined> {
  return undefined;
}
```

Pass the actual cached runtime into the hook. The trusted host constructs and initializes its journal with that runtime and the same deployment D1/fence. Cache the reader after successful initialization rather than constructing another workflow runtime during each request. Initialization failure refuses execution. A host without the hook refuses the continuation operation before engine execution.

Return an additive `DoRunRemoteStepTopology` subtype from `createDoRunTopology`, and expose it in the pre-route kit:

```ts
interface DoRunRemoteStepTopology extends DoRunLifecycleTopology {
  resumeWorkflowRemoteStep(
    workflowId: string,
    runId: string,
    invocationId: string,
    principal: ExecutionPrincipal,
    mutationEpoch?: number,
  ): Promise<WorkflowRemoteStepResumeResult>;
}
```

The successful response distinguishes a witnessed application from a retry after settlement:

```ts
type WorkflowRemoteStepResumeResult =
  | { kind: 'applied'; invocationId: string; summary: RunSummary }
  | { kind: 'already-applied'; invocationId: string };
```

The `already-applied` response reports the persisted settlement, without returning a later summary as the original result. Callers can read the run’s current status separately. Validate the topology response’s discriminant, invocation ID and summary run ID in the existing response module. Preserve its error mapping and exclude private storage/provenance fields.

Extend `createWorkflowRemoteStepPreRoute` with `POST /_flowsafe/workflow-remote-step/resume`. Its exact body is `{invocationId}`. Authenticate with the existing private credential before reading the journal. Use `journal.getWorkflow` and `kit.contextForStoredWorkflowInvocation` to validate the invocation’s target, ID, deployment, prefix and stored principal. Compare principals with `samePrincipal` from `src/approval-api/principal.ts`.

Read `context.resourceOwnerFor('run', invocation.runId)` before deciding whether to forward. A present owner must match the invocation principal through the existing `isOwnedBy` helper, then pass `requireResourceAccess(..., 'write', 'run')`. Refuse a conflicting owner even if a broader actor policy would otherwise allow access. When the owner is absent, forward only an exact `targetKind === 'workflow'`, `status === 'settled'`, `settlementOutcome === 'applied'` invocation whose identity and principal passed the preceding checks. The RunDO must revalidate retained start authority before returning `already-applied`; this exception cannot claim or execute a continuation. First claims and unresolved invocations still require normal run write access.

Do not call `dispatch.getWorkflowRemoteStep` before this decision: its resource-read check would refuse an ownership-released settlement before the RunDO could verify it. Use the existing journal reader and stored-context helper for this operation, preserving the other dispatch operations’ authorization. Do not add a RunDO namespace to `AgentThreadTopology`.

The topology sends `POST /runs/:workflowId/:runId/remote-step-resume` to the existing `workflowId:runId` object. Stamp the deployment-identity credential, encoded execution principal and captured mutation epoch using existing helpers. Its exact body is `{invocationId}`. The caller does not select result bytes, a start token, a step or a suspension timestamp.

### Result envelope and ordinary-resume refusal

Deliver retained bytes to the trusted workflow through a reserved envelope. The application can parse its own result schema without reading private journal or snapshot fields:

```ts
export const WORKFLOW_REMOTE_STEP_RESULT_KEY =
  'flowsafe.workflowRemoteStepResult';

export interface WorkflowRemoteStepResumeData {
  [WORKFLOW_REMOTE_STEP_RESULT_KEY]: {
    version: 1;
    invocationId: string;
    requestDigest: string;
    resultJson: string;
    resultDigest: string;
  };
}
```

Construct this value inside the RunDO after validating the stored invocation and result. Preserve `resultJson` byte-for-byte and use the journal’s verified digest. The marker selects the format; it supplies no independent authority. The interpreter must treat result content as application data and never derive credentials, principals or grants from it.

Before: ordinary `/resume` rejects the reserved suspension-timeout key through `DurableObjectRunner.#resumeData`. After: the same boundary also rejects `WORKFLOW_REMOTE_STEP_RESULT_KEY`. Use the existing error type. The private continuation constructs the envelope after that public-input boundary and calls the runtime directly under the RunDO lock. Do not pass that internal envelope through the ordinary-input rejection helper.

ASP also denies ordinary resume for its fixed customer workflow through its host policy. No general public resume capability is created. Export the types and constant from `do-runner` and the relevant consumer types from `agent-host`; keep `do-runner` independent of `agent-host` imports.

### Admission and execution under the owning lock

The RunDO performs this sequence inside its existing `#withOperationLock`:

1. Reassert the object’s `workflowId:runId` name, parse the exact invocation body and decode the trusted principal. Obtain the opt-in reader and workflow invocation.
2. Compare the invocation’s deployment, D1 prefix, workflow/run IDs and principal with the configured reader and authoritative start state. Require a D1 workflow target, the same start token and initiating owner, and absent agent-start provenance. Recheck `runOwnership(this.env).owner('run', runId)` inside the lock. A present conflicting owner refuses. An absent owner permits only a matching `settled/applied` receipt after the retained start-principal checks; it grants no execution authority. Other states require the current matching ownership entry.
3. Return `already-applied` for a matching `settled/applied` invocation without an engine call. Refuse `resuming` even after lease expiry. Refuse other unavailable terminal states without relabeling them applied.
4. Require `result-retained`, a valid retained result, an unexpired host/run deadline and an exact current workflow suspension. Match path, timestamp, resume count, marker, request bytes and digest.
5. Admit top-level suspension paths only. Refuse nested-workflow metadata and ambiguous paths. For this release, require one path segment without a dot; this avoids the stored projection’s ambiguity between a dotted step ID and a nested path. Existing permit/result retention remains compatible, but an older unsupported suspension cannot gain continuation authority.
6. Claim with one conditional D1 update. Require `target_kind = 'workflow'`, `operation_state IS NULL`, the stored identity, principal, request/result digests, permit generation, write revision/generation, deadline and exact authoritative snapshot. Include the observed execution-fence predicate and validated caller epoch. Set `status = 'resuming'`, assign the lease owner, and increment both write counters. A zero-row update refuses execution.
7. Invoke `runtime.resume` through `#withLeg` with the stored principal, selected path, `expectedSuspension` and reserved result envelope. Preserve the existing existing-run fence checks. Neither the route nor the result selects trusted request context.
8. Pass the returned summary to workflow settlement under the same lock. Require the same lease owner, target identity, generation and retained result. Mark `applied` only when the witnessed advancement matches the authoritative run state for that start token. A witnessed terminal failure records `terminal-not-success`; it never reports applied.
9. Reconcile suspension deadlines and approvals and broadcast the summary through the existing ordinary-resume boundary operations. Do not create a second alarm scheduler or execution lock.

Before: copying agent settlement’s `same(current) → 'retry' → 'result-retained'` would reopen admission. After: workflow settlement leaves the row `resuming` when the outcome is unchanged, unreadable or unproved. It returns `unresolved`; the private route returns an error without another engine call. A throw after claim also leaves the claim intact.

### Restarts, drain and schema compatibility

The state transition decides whether a fresh process may execute:

| Stored observation | Required behavior |
| --- | --- |
| Retained result without a resume claim | Permit a first claim after revalidating current authority |
| Concurrent first claims | Admit one conditional write |
| Persisted claim before an interrupted response | Keep `resuming`; do not infer whether the engine entered |
| Expired lease or replacement isolate | Refuse reclaim and engine replay |
| Unchanged suspension after admission | Keep the claim unresolved |
| Advanced snapshot without this call’s matching witness | Keep the claim unresolved |
| Witnessed matching advancement | Record `settled/applied` |
| Witnessed matching terminal failure | Record `failed/terminal-not-success` |
| Lost response after successful settlement | Return `already-applied` without execution |
| Successful settlement followed by cancellation/timeout ownership cleanup | Forward the matching settled receipt despite the absent owner; the RunDO revalidates retained start authority and returns `already-applied` |
| Settled receipt with a present conflicting owner | Refuse at the Worker and RunDO boundaries |

An unresolved row retains its result, lease and `resuming` state, so the existing remote-step inventory includes it. Do not convert uncertainty to a terminal outcome that inventory omits. Deadlines, redeploys and retention must not erase the record preventing replay. This checkpoint does not implement an operator-resolution endpoint or automatic recovery scanner.

The v5 schema already has the required fields and write-generation trigger. Preserve its exact validation, migration history and old-writer refusal. Existing 0.29 code cannot claim workflow continuations, and it can still inventory their rows. Do not add workflow rows to the agent recovery scan or notification-operation protocol. Rollback disables this optional host route while retaining admitted work.

### Checkpoints and release

Implement these coherent checkpoints after one design-level review of this supplement:

1. **Workflow journal admission and settlement.** Add the workflow-only methods and reader/envelope types, with SQLite tests for the state transitions and invariants above. Keep agent behavior and schema unchanged.
2. **Private RunDO continuation.** Add the optional hook, run-topology operation, private route, envelope refusal and lifecycle integration. Prove stored authority and single execution through the real RunDO path. Add the minor FlowSafe changeset and public behavior documentation.
3. **Packed and native consumer proof.** Extend the existing packed consumer and RS3 workerd scenario to execute this public package surface across process restart. Update this plan’s outcome once the proof and release gates pass.

Use a subsequent minor release after 0.29.0 through the repository’s feature-to-`dev`, version-PR and `dev`-to-`main` flow. Do not modify the already verified 0.29.0 release worktree or publish replacement bytes under that version. ASP consumes the subsequent published package after its verification; ASP does not duplicate this runtime implementation.

### Verification and completion criteria

Apply `test-audit` before writing tests. State the behavior, regression and missing coverage. For a claimed bug fix, retain a pre-fix failure caused by that bug; this additive feature can use direct new-behavior coverage without claiming a pre-existing regression.

Select tests by the changed implementation and contracts:

- `src/do-runner/remote-step-journal.test.ts`: workflow claim/settlement, competing callers, wrong lease, changed principal/start/suspension/request/deadline/fence, reopened retained result and refused expired claim. Preserve agent-only `claimResume` refusal for workflow rows.
- `src/do-runner/durable-object.test.ts`: a real registered workflow consumes its recorded result; ordinary resume cannot forge the envelope; foreign run/object/principal refuses; duplicate requests execute once; an admitted unresolved call remains blocked after object recreation. Settle to another suspension, cancel or time out the run through normal cleanup, and verify a retry returns `already-applied` after ownership release without another engine call. Refuse changed retained start identity and a present conflicting owner.
- `src/host-kit/do-run-topology.test.ts` and the existing private-route coverage in `src/host-kit/flowsafe-worker.test.ts`: authenticated forwarding, stored actor authority, write access, exact bodies and refusal before execution. Verify an absent owner forwards only a matching settled/applied invocation, while absent-owner first/unresolved claims and present conflicting owners refuse. Couple this route coverage to the RunDO cleanup regression so the completed receipt is reachable through the composed path. Derive additional test targets from actual changed shared helpers.
- `scripts/agent-host-pack-test.mjs`: compile the protected hook and exported contracts against the tarball; exercise the continuation path without source imports or private storage access.
- `spike/worker.ts` and `scripts/spike-verify.mjs`: extend RS3’s real private workflow to consume the envelope. Retain a result, restart workerd, resume through the exported private route and assert a concrete successful result. Retry and restart again to prove no second continuation. Keep a separately identified admitted-but-unsettled scenario that refuses a second engine call; an injected ambiguous fixture is not evidence of an actual mid-engine crash.

Run the selected unit files with the owning configuration:

```bash
fnm exec --using 22.22.3 -- pnpm --filter @proofoftech/flowsafe exec \
  vitest run src/do-runner/remote-step-journal.test.ts \
  src/do-runner/durable-object.test.ts \
  src/host-kit/do-run-topology.test.ts \
  src/host-kit/flowsafe-worker.test.ts
```

Use the smallest configured TypeScript/build targets and the packed/native proofs when their checkpoint changes:

```bash
fnm exec --using 22.22.3 -- pnpm --filter @proofoftech/flowsafe typecheck:test
fnm exec --using 22.22.3 -- pnpm --filter @proofoftech/flowsafe typecheck:spike
fnm exec --using 22.22.3 -- pnpm --filter @proofoftech/flowsafe build
fnm exec --using 22.22.3 -- pnpm test:packed-flowsafe-agent-host
fnm exec --using 22.22.3 -- pnpm --filter @proofoftech/flowsafe spike:verify
```

Run Biome against the changed TypeScript/JavaScript paths with the repository configuration. Exported type/protocol changes select the owning FlowSafe suite once under the quality gate’s shared-contract exception. Do not duplicate already valid check evidence. After the planned checkpoints close, run the [complete maintainer gate](../maintainer-guide.md#verification) once on the accumulated release tree, using local runs under the owner’s current no-Actions instruction.

Every implementation checkpoint is T2: independent QA, clean-code, architecture and security lanes. Review the RunDO’s existing resume/alarm/ownership paths, journal inventory/retention contracts and private Worker authorization as reach. Each check writes a new literal `/tmp` log path, records the checked tree/commit and ends with the actual `EXIT_STATUS`. The plan’s design review does not run or simulate these commands.

Completion requires a verified package consumer to deliver the retained result once through the owning RunDO, reject forged ordinary resume, preserve unresolved admission across restart, and leave agent behavior intact. Native workerd proof does not replace ASP’s live WFP grant, result and paid-launch proofs.

### Excluded work and rejected alternatives

The supplement excludes these changes:

- Automatic Mastra replay or lease stealing: the pinned engine lacks the required attempt-generation persistence guarantee.
- Dynamic workflow registration or executing customer modules in the state Worker: ASP uses a static trusted interpreter and its separate WFP execution boundary.
- Synthetic agent/thread/resource identities: they route workflow authority through the wrong host and cleanup contracts.
- Direct Worker-side `runtime.resume`: it bypasses the owning RunDO’s operation lock and lifecycle markers.
- Parallel permit/result tables or raw ASP D1 access: they duplicate the journal authority and couple ASP to private storage.
- New notification operations, general effect recovery, schema migration or exact historical summary storage: normal single admission does not require them.
- Changes to current agent continuation semantics: keep that separately reviewed protocol unchanged.

## Appendix: historical journal anchors

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

## Appendix: continuation anchors

These anchors were read at `91db19efe32cc87ef669eb7350198d10a0511cba`. Paths below are relative to `packages/flowsafe/`. They describe the continuation supplement’s baseline, not the historical journal implementation above.

### Target selection and retained bytes

`src/do-runner/remote-step-journal.ts:1690`, `RemoteStepJournal.get` and `getWorkflow`, distinguishes the target before decoding:

```ts
async get(invocationId: string): Promise<RemoteStepInvocation | undefined> {
  const row = await this.#getRow(invocationId);
  return row?.target_kind === 'agent' ? decode(row) : undefined;
}

async getWorkflow(
  invocationId: string,
): Promise<WorkflowRemoteStepInvocation | undefined> {
  const row = await this.#getRow(invocationId);
  return row?.target_kind === 'workflow' ? decodeWorkflow(row) : undefined;
}
```

Preserve these selectors. `claimResume` at line 2505 and `settleAfterResume` at line 2557 each contain `const invocation = await this.get(invocationId);`. Their workflow counterparts use `getWorkflow` and workflow-specific predicates; changing `get` to return either target would misroute the existing agent host.

`RecordedRemoteStepResult` at line 310 already defines the retained bytes and evidence:

```ts
export interface RecordedRemoteStepResult {
  invocationId: string;
  resultJson: string;
  resultDigest: string;
  quiescenceDigest: string;
  executorQuiescentAt: number;
}
```

`readRetainedResult` at line 2105 calls `decodeAny(row)` and `#retainedResultFromRow`; the latter checks the JSON digest before returning this value. Reuse that path, without exposing row fields to ASP or reading settled results as active continuation authority.

### Exact workflow suspension and epoch

`selectedWorkflowSuspension` at line 780 currently selects the marker without excluding nested paths:

```ts
return suspended?.type === 'suspension' &&
  remoteStep?.marker === WORKFLOW_REMOTE_STEP_MARKER
  ? [{ path, key, remoteStep }]
  : [];
```

Preserve registration/retention compatibility. At workflow continuation admission, require the selected stored/live path to contain one non-dotted segment and refuse `MASTRA_WORKFLOW_META_KEY` nesting metadata. Validate both the live projection and stored suspension; a projection that collapses a nested path must not pass.

`#admission` at line 1792 already returns the observation used by its SQL predicate:

```ts
const observed = await this.#fence.readForAdmission();
const proof = (await this.#execution(invocation, invocation.principal))
  .execution;
if (proof.startToken !== invocation.startToken)
  throw new RemoteStepRefusal(409, 'remote step run generation changed');
if (!admitsExistingRun(observed.reading, proof))
  throw new RemoteStepRefusal(503, 'remote step execution is fenced');
```

Its return is `return { predicate, values, bind, observed: observed.reading };`. The new claim uses that observation directly:

```ts
const gate = await this.#admission(invocation, base.length);
assertMutationEpoch(gate.observed, expectedMutationEpoch);
```

Bind `gate.values` into the conditional update containing `gate.predicate`. This is the before-to-after difference from separately checking an epoch and later accepting a different fence observation.

### Lease settlement without readmission

`settleAfterResume` at line 2557 contains this unchanged-suspension transition:

```ts
if (same(current))
  outcome =
    this.#now() >= invocation.deadlineAt ? 'deadline-expired' : 'retry';
```

Its update binds `outcome === 'retry' ? 'result-retained' : ...`. Keep that existing agent path unchanged. The new workflow settlement instead follows this decision:

```ts
if (same(current) || !matchesWitness(witnessed, current)) {
  return 'unresolved';
}
```

`same` and `matchesWitness` here describe implementation-local comparisons, not proposed exports. This return performs no update, retains the lease and leaves `status = 'resuming'`. The successful conditional update checks the exact lease owner, target, start token, permit generation, retained digest and write counters. It does not reclaim or renew an expired claim into another owner.

`src/do-runner/inventory.ts:1099` includes nonterminal journal states through `status NOT IN ('failed', 'settled')`. Preserving `resuming` keeps uncertainty in drain inventory without a schema or inventory change. The v5 schema in `remoteStepTableSql` already has `resume_lease_owner`, `resume_lease_expires_at`, `settled_at`, `settlement_outcome`, `target_kind` and `write_generation`.

### Private route and object execution boundary

`src/agent-host/workflow-remote-step-route.ts:30` currently declares:

```ts
type Operation = 'register' | 'ticket' | 'consume' | 'result';
```

Extend that exact-body operation table with `resume: ['invocationId']`. Preserve its authorization before body/state work, bounded body, query rejection and no-store response. `src/host-kit/flowsafe-worker.ts:1753` supplies the private gate:

```ts
const gate = await authorizeAdminCredential(privateRequest, env, {
  surface: 'private workflow dispatch',
  delegateCapability: false,
});
```

`contextForStoredWorkflowInvocation` at line 1772 adapts `journal.getWorkflow` into the existing stored-principal context path. Reuse it, then distinguish current run ownership from retained settlement readback before forwarding.

`src/approval-api/actor-context.ts:187`, `createPrincipalActorContext`, exposes the owner lookup and refuses resource access when the owner is absent:

```ts
resourceOwnerFor: (kind, resourceId) => resources.owner(kind, resourceId),
canAccessResource: async (kind, resourceId, access) => {
  const stored = await resources.owner(kind, resourceId);
  return (
    stored !== undefined && principalMayAccess(principal, stored, access)
  );
},
```

`src/do-runner/durable-object.ts:699`, `#completeTerminalEffects`, releases that ownership after cancellation/timeout through `ownership.release('run', runId, owner)`. Before the design correction, unconditional `requireResourceAccess` made the retained receipt unreachable. After the correction, the Worker applies this decision to the validated workflow invocation:

```ts
const owner = await context.resourceOwnerFor('run', invocation.runId);
if (owner !== undefined) {
  if (!isOwnedBy(owner, invocation.principal)) {
    throw new RunRouteError(404, 'run not found');
  }
  await requireResourceAccess(
    context, 'run', invocation.runId, 'write', 'run',
  );
} else if (
  invocation.status !== 'settled' ||
  invocation.settlementOutcome !== 'applied'
) {
  throw new RunRouteError(404, 'run not found');
}
```

The surrounding checks establish the exact workflow target and stored principal before this branch. `isOwnedBy` in `src/approval-api/principal.ts:389` compares owner kind and ID; `samePrincipal` at line 365 compares the full principal. The RunDO repeats ownership and retained-start validation under its operation lock, so a change between Worker forwarding and object execution cannot obtain first-admission authority.

`src/do-runner/durable-object.ts:592`, `#resumeData`, currently refuses the timeout envelope with this predicate:

```ts
value !== null &&
typeof value === 'object' &&
SUSPENSION_TIMEOUT_RESUME_KEY in value
```

Extend the ordinary-resume refusal to cover the new reserved result key:

```ts
value !== null &&
typeof value === 'object' &&
(SUSPENSION_TIMEOUT_RESUME_KEY in value ||
  WORKFLOW_REMOTE_STEP_RESULT_KEY in value)
```

`#withOperationLock` at line 812 waits on `#operationTail` and releases it in `finally`. Ordinary resume at line 2883 enters that lock and wraps `runtime.resume` with `#withLeg(..., 'resume', ...)`. Place the workflow claim, resume and settlement within the same lock and leg lifecycle. Calling the runtime from the Worker route would omit these boundaries.

`#startPrincipalReceipt` at line 1052 verifies the stored receipt against authoritative D1 start provenance, including `identity?.target.kind !== 'workflow'`, the workflow ID, owner and start token. Reuse those checks when normal terminal cleanup no longer has an active ownership row; the private invocation alone does not authorize another run generation.

### Native and packed proof gaps

`spike/worker.ts:875`, workflow `spike-private-remote`, currently suspends unconditionally in its `execute` step:

```ts
execute: async ({ inputData, suspend }) =>
  suspend({
    type: 'suspension',
    remoteStep: {
      marker: WORKFLOW_REMOTE_STEP_MARKER,
      request: { version: 2, input: { query: inputData.query } },
    },
  }),
```

Extend this existing fixture to validate and consume the reserved resume envelope before returning a concrete result. `scripts/spike-verify.mjs:3699` names RS3 as “private workflow route: trusted start, one permit winner, retained result and restart refusal”; its current last assertions cover consumed-permit and retained-result replay after restart. Add continuation and duplicate/refused-admission observations there.

`scripts/agent-host-pack-test.mjs:1386` currently checks `await remoteStepJournal.getWorkflow('absent-invocation')` and a GET ticket response of 405. Extend the packed consumer to compile and exercise the opt-in RunDO continuation through exported APIs; those existing observations cannot prove continuation.
