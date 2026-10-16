# Recover a remote-step resume without repeating an external effect

> Content type: design plan. Audience: FlowSafe maintainers and deployment hosts. Status: proposed, not implemented.

The dated amendment at the end is the current execution plan. The preceding sections preserve the decision and code baseline from 2026-10-12.

This plan records the limit of safe cross-instance resume recovery on pinned `@mastra/core@1.73.0`. Its `run.resume` can execute arbitrary tool closures, nested agents, background work, model calls and direct network effects outside a FlowSafe-owned adapter. General agent continuation is therefore ineligible for automatic replay. A narrow host-owned operation with an explicit, closed effect set may use a downstream idempotent executor; the subsequent Mastra continuation remains single-admission. An uncertain Mastra continuation stays unresolved and blocks drain until independent evidence or a future runtime contract resolves it.

## Baseline and evidence

- Branch: `plan/flowsafe-remote-step-broker`; base commit: `4096fbd` (2026-10-12). The settlement checkpoint at that commit leaves expired `resuming` rows unresolved and in drain inventory. Architecture pass 3 cleared the lease-reclaim MUST in `/tmp/asp-flowsafe-settlement-arch-pass3-20261012.md`. Reconfirm the appendix against the checked-out commit before editing.
- Runtime: Node and pnpm versions in the root manifest; Cloudflare Workers, D1 and Durable Objects in production. Use a linked worktree with the shared pnpm store. Keep checks in `/tmp` logs. The full FlowSafe suite passed 6,648/6,648 for the committed settlement content. Automatic cross-isolate replay remains a release gate.
- Cloudflare [Durable Object lifecycle](https://developers.cloudflare.com/durable-objects/concepts/durable-object-lifecycle/) says shutdown can route new requests to a new instance while an old HTTP/RPC request continues if it avoids object storage. A later old-instance storage access fails. Cloudflare [alarms](https://developers.cloudflare.com/durable-objects/api/alarms/) run at least once and have bounded automatic retries. Neither an in-memory lock, a lease timeout, an alarm, nor a Worker deploy proves that an old continuation or its external effect stopped.
- FlowSafe's [README runtime section](../../packages/flowsafe/README.md) and [interrupted-leg design](../do-runner-design.md#interrupted-legs) say a deploy can leave an outgoing leg running and that an aborted external call may already have taken effect. This plan treats that as part of the supported execution model.
- The installed `@mastra/core@1.73.0` source executes `tool.execute` directly in `dist/agent/durable/index.js` and `dist/workflow-event-processor-LHwOlFwA.js`; `dist/agent-D-eTzWxF.js` enters `run.resume` and nested agent paths. Its engine persistence includes `persist`, `replace` and `patch` paths without a remote-step attempt generation. The appendix anchors the exact installed source. The proposed generic effect adapter and runtime generation seam fail this version's capability check.

## Decision and invariant

**Recommend a host-owned closed-operation profile.** Its declared operations run outside arbitrary resumed Mastra closures, through a concrete downstream executor. The host enumerates every operation and its inputs before admission. Each effect uses an operation key that stays identical across retries of that logical operation. The downstream system must atomically deduplicate the key with the effect and return the first operation's durable outcome. A preliminary read, best-effort cancellation or FlowSafe-only fence does not establish this property. If the external system supports a fencing generation instead, it must reject stale generations in the same atomic operation as the effect and expose a durable outcome lookup. The profile never authorizes automatic replay of `run.resume` on the pinned core.

No downstream executor is selected in this repository yet. The profile remains a gated proposal until an owner names one and proves its exact idempotency and receipt API. The existing general agent path stays fail closed.

A `resumeAttemptId` identifies an execution attempt and helps attribute logs. It must not become the external idempotency key, because retries would use a new attempt ID. The stable effect key binds deployment, run generation, remote invocation and an enumerated host operation. The host persists that operation and its request digest before calling the executor. The executor returns a durable receipt or an explicit unresolved state. It must never mark a timed-out request as not executed because the response was lost.

One retained result remains bound to one exact suspension. On resume, preserve the stored principal, thread/resource binding, start token, run ID, step path, suspension timestamp/count, mutation epoch, deadline, result digest and current deployment fence. Host operations may be retried through their downstream idempotency contract. Mastra `run.resume` may be admitted once; a newer instance must not invoke it again merely because a lease expired or the snapshot still says suspended. `RunnerRuntime.resume` receives no remote-step attempt generation through `resumeViaRuntime`, and the pinned engine does not fence competing snapshot writes. An unresolved first call remains `resuming`/`recovery-blocked` inventory work.

An advanced snapshot alone does not prove which attempt or retained result produced it. A crash after the snapshot write loses the in-memory `resumeViaRuntime` witness. The pinned engine has no proven attempt-linked receipt stored atomically with every advancing snapshot write. Without that receipt, classify the advanced state as unresolved rather than `applied`; an operator needs independent evidence. A future runtime may lift this limit only after its entire persistence path atomically binds generation, exact suspension and result digest to the advancing write.

### Considered options

| Option | Decision |
| --- | --- |
| Extend the in-instance `withDispatchLock` and reclaim expired leases | Reject. It cannot serialize an outgoing object instance after deploy. |
| Wait a fixed grace period or rely on a Durable Object alarm | Reject. Time and alarm delivery do not prove quiescence or external outcome. |
| Check the authoritative snapshot and retry when it still says suspended | Reject for arbitrary effects. The old continuation can be in flight without a completed snapshot write. |
| Require a universally transactional external operation | Reject as a generic FlowSafe feature. FlowSafe does not control downstream systems. |
| Generic resumed Mastra tool adapter | Reject on pinned 1.73.0. Direct tool closures and nested work bypass it, and snapshot writes lack attempt generation. |
| Host-owned closed operations with stable, atomically enforced effect keys and durable receipts | Recommend for the external operation before single-admission continuation. Hosts lacking a concrete executor retain a visible unresolved obligation. |

No new dependency is proposed. Reuse the existing journal, D1 transaction and conditional-write patterns, runtime status reader, host authentication, execution fence, and inventory. Any later package addition needs the repository package-first and supply-chain checks.

## Recovery protocol

1. Before calling the host-owned executor, the trusted host verifies the retained result and current authoritative run. In one D1 transaction, it records the closed operation list, full run/suspension identity, result digest and stable executor keys. A callback payload cannot select the profile or add an operation. A row whose previous host operation is unresolved can be retried only under the executor's atomic idempotency contract. A row whose Mastra resume was admitted cannot be resumed again automatically.
2. For each host operation, persist its key and request digest before sending. The downstream executor atomically records the effect and first result under that key. A repeated key and identical request returns its durable receipt; a changed request fails closed. After a lost response, query by key or retry with the same key. If the executor cannot establish the effect outcome, mark the row unresolved and block drain.
3. After all host receipts are durable, atomically record a one-time `resume-admitted` decision bound to the exact suspension and retained result. Then call the existing trusted `resumeViaRuntime` path once. A crash between admission and the call is intentionally ambiguous and blocks retry. If the call returns, `settleAfterResume` may use its returned witness and authoritative status to settle; a missing witness, unreadable run or ambiguous advancement remains unresolved. No host receipt proves that an arbitrary Mastra tool executed once.
4. On crash or alarm, scan bounded active work and route inspection to the owning thread host. Reconcile only host operations whose downstream executor enforces stable keys. If Mastra resume was admitted, read the journal and authoritative snapshot for diagnosis but do not call `run.resume` again or infer `applied` from an advanced snapshot. Record independent operator evidence before resolving a missing witness. A future automatic continuation protocol requires a separately reviewed runtime version that fences every competing snapshot write and persists attempt-linked provenance atomically with advancement; the pinned version fails this gate.
5. Keep host receipts and uncertain resume records in drain inventory across deploy, rollback, deadline, retention cleanup and migration. An automatic purge cannot erase the evidence that prevents replay. An operator resolution records the independent external evidence and must not invent `applied` from elapsed time.

The protocol needs one host-only recovery method and a versioned journal migration. Its endpoint or method must use the same internal authentication and stored-principal checks as the current remote-step host route. It must not be a public result route. Versioned schema migration preserves old rows; a legacy `resuming` row without durable attempt/effect evidence is ambiguous and remains blocked. Refuse unknown schema versions. During mixed-version rollout, old code must not claim or settle new-version attempts; stage code and migration so the old version fails closed or keep remote-step dispatch disabled until cutover. Prove that with an actual old/new Worker-version exercise, not an inferred migration rule.

### Before and target contracts

The current `RemoteStepJournal.claimResume` writes an expiring lease for the initial result-retained claim, then the host calls the runtime:

```typescript
const leaseOwner = await reader.claimResume(invocationId, 60_000);
const resumed = await withExecution(execution, () =>
  durable.resumeViaRuntime({
    runId: ref.runId,
    requestedBy: stored.principal.id,
```

The current `RemoteStepJournal.reconcileResult` can settle a retained result but returns `pending-quiescence` for `resuming`:

```typescript
if (invocation.status === 'resuming') return 'pending-quiescence';
const owner = await this.claimResume(invocationId, 60_000);
return this.settleAfterResume(invocationId, owner);
```

The target is an **internal contract sketch**, not a claim that these methods exist. It shows where the host-owned operation can safely retry and where Mastra continuation cannot:

```typescript
const operation = await reader.prepareHostOperation({
  invocationId,
  trustedClosedProfileId,
});
const receipt = await executor.executeIdempotent(operation);
await reader.recordHostReceipt(operation.id, receipt);
await reader.admitSingleResume(invocationId, operation.id);
const resumed = await withExecution(execution, () =>
  durable.resumeViaRuntime(boundResumeRequest),
);
await reader.settleAfterResume(invocationId, operation.id, resumed);
```

The exact method names and types require implementation design. `admitSingleResume` must persist before `resumeViaRuntime`; any error or crash after admission leaves an unresolved record. The closed executor is host-owned, not a wrapper around arbitrary `tool.execute`. Do not weaken existing approval resume or ordinary agent run APIs to fit this sketch.

The pinned runtime lacks the generation and provenance path required for automatic `run.resume` replay. `FencedWorkflowsStorageD1.persistWorkflowSnapshot` alone cannot fence all engine writes. A future protocol must inspect `RunnerRuntime.resume` and every installed engine persistence path, including persist, replace and patch, then prove atomic generation refusal and provenance. This plan does not implement that future protocol.

## Implementation checkpoints

1. **Capability result and host contract:** The pinned `@mastra/core@1.73.0` audit failed the generic adapter and generation proof. Do not re-run the same proof expecting an arbitrary Mastra continuation to become safe. Identify a concrete host-owned closed operation and executor, list every effect it can perform, and prove atomic idempotency and receipt lookup for that executor. If no concrete operation meets the contract, leave the existing fail-closed result path in place and defer this profile.
2. **Journal and host operation state:** Add a versioned migration, stable operation keys/receipts, one-time `resume-admitted` marker, active index/inventory, and host-only inspection method. Keep old rows intact and ambiguous rows blocked. Authenticate the method; re-read principal, binding, start token, epoch, deadline and authoritative suspension. Keep the current host lock for same-instance ordering, but do not use it as cross-instance evidence.
3. **Closed executor and recovery duty:** Add the host-owned executor only after its downstream idempotency contract is proven. Wire bounded scan and alarm/maintenance wake-up to inspect and reconcile host operations. The duty never re-invokes Mastra after `resume-admitted`. Keep alarm scheduling compatible with the host's existing alarm owner, because a Durable Object has one alarm slot.
4. **Cutover and proof:** Exercise old/new versions, crash and lost acknowledgement on real D1/workerd storage. Keep feature disabled for customer dispatch until the proof matrix below passes. Merge to `dev` through reviewed checkpoints and publish through the Anchorage release process only after the release gates pass.

## Verification and release gates

Run each checkpoint's narrowest changed-file checks once, in `/tmp` logs with the content ID and final `EXIT_STATUS`. For the shared journal schema/protocol checkpoint, the quality gate's shared-contract exception selects the owning FlowSafe package suite. Candidate commands, adjusted to the changed-file list at execution time:

```bash
pnpm --filter @proofoftech/flowsafe exec vitest run \
  src/do-runner/remote-step-journal.test.ts \
  src/agent-host/thread-host.test.ts \
  src/agent-host/thread-topology.test.ts
pnpm exec biome check packages/flowsafe/src/do-runner/remote-step-journal.ts \
  packages/flowsafe/src/agent-host/thread-host.ts
pnpm --filter @proofoftech/flowsafe typecheck:test
pnpm --filter @proofoftech/flowsafe build
pnpm --filter @proofoftech/flowsafe test
```

The gate owner redirects each command's complete output to a distinct `/tmp` log, appends its exit status, and gives reviewers the paths. After all checkpoints, run the repository's full suite once, including direct scenarios where the changed contract reaches them. Do not substitute mocked D1 or a second object in one process for the real fresh-isolate proof.

| Proof | Required observation |
| --- | --- |
| Two resumes in one object instance | One attempt begins; the other sees the durable state, with no duplicate effect. |
| Deploy while old instance is inside a host-owned external operation beyond the 60s lease | New instance never treats lease expiry as quiescence. A repeated host operation uses the stable key and returns one durable executor outcome. |
| Crash before host effect, after effect before receipt, and after receipt before Mastra admission | The executor receipt resolves the host operation or the row stays blocked; no ambiguous effect is repeated under a new key. |
| Crash after `resume-admitted` before call, during call, or after advancing snapshot before journal settlement | No automatic second `run.resume` occurs. A missing returned witness stays unresolved even if the snapshot advanced; operator evidence is required. |
| Identical and conflicting retries | Same operation key plus same request yields the original receipt; changed request fails closed. |
| Old Mastra attempt remains in flight after deploy | No newer Mastra attempt starts; journal and drain inventory preserve the uncertainty. |
| Changed principal, resource mapping, start token, suspension, epoch, deadline, or fence | Recovery refuses before any effect. |
| Legacy and unknown schema rows | Known rows survive migration; ambiguous `resuming` rows stay visible and blocked; unknown versions refuse. |
| Alarm duplicate, exhausted retries, process restart, and no traffic | Maintenance remains scheduled and scans bounded active work without relying on a single alarm delivery. |
| Migration drain and retention | Active/blocked rows remain inventory work; cleanup cannot erase unresolved effect evidence. |
| Closed-profile bypass | An operation outside the enumerated host contract is rejected; arbitrary `tool.execute` remains ineligible for automatic replay. |

Every implementation checkpoint is T2 for concurrency, authentication, migration and data loss: independent clean-code, QA, architecture and security lanes. This plan received one design-level review when added; that review does not run or simulate its commands. No MUST may remain when a checkpoint closes. A live release requires the concrete executor's idempotency proof and a real old/new Cloudflare rollout exercise. Without those proofs, retain the fail-closed state. Even with them, advertise recovery only for the host-owned operation, not automatic Mastra continuation.

## Reuse map and excluded work

- `packages/flowsafe/src/do-runner/remote-step-journal.ts`: canonical suspension identity, retained result digest, D1 migration, conditional settlement and bounded recovery scan
- `packages/flowsafe/src/agent-host/thread-host.ts`: trusted internal route, stored-principal authorization, bound runtime resume and same-instance dispatch lock
- `packages/flowsafe/src/agent-host/thread-topology.ts`: owning thread routing and trusted host context
- `packages/flowsafe/src/do-runner/execution-fence.ts`: deployment epoch and drain boundary
- `packages/flowsafe/src/do-runner/inventory.ts`: active obligation visibility
- `packages/flowsafe/src/agent-runner/durable-agent-runner.ts`: pinned runtime continuation and authoritative status path
- `packages/flowsafe/src/do-runner/runtime.ts`, `RunnerRuntime.resume`: runtime re-entry that currently lacks a remote-step attempt generation
- `packages/flowsafe/src/do-runner/fenced-workflows-d1.ts`, `FencedWorkflowsStorageD1.persistWorkflowSnapshot`: partial storage seam; its existence does not authorize replay

The plan does not change the public result-intake API, approval decision semantics, arbitrary agent tools, customer signing, Fleet Control ownership, payment policy, or external executor implementation. Those are separate owners or cannot meet this replay contract without their own proof. It also does not turn an unknown effect outcome into a terminal success or failure; doing so would hide an unresolved obligation.

## Appendix: current-state anchors

The FlowSafe excerpts come from `4096fbd`; the installed Mastra excerpts come from the locked `@mastra/core@1.73.0` package. Reconfirm symbols and semantics, not line numbers.

- `packages/flowsafe/src/agent-host/thread-host.ts`, `createThreadAgentHost` dispatch and remote-step route:

```typescript
const withDispatchLock = createFifoLock();
```

The route later calls `withDispatchLock`; the lock lives in one host object instance. The remote-step route currently refuses an active `resuming` row:

```typescript
if (invocation.status === 'resuming')
  throw new AgentHostRequestError(
    503,
    'remote step resume awaits executor quiescence',
  );
```

- `packages/flowsafe/src/do-runner/remote-step-journal.ts`, `claimResume` and `settleAfterResume`:

```typescript
SET status = 'resuming', resume_lease_owner = ?, resume_lease_expires_at = ?
```

```typescript
AND resume_lease_owner = ? AND permit_generation = ?
AND permit_epoch IS ? AND start_token = ?
```

These predicates protect the journal row but do not prove the external effect or the outgoing runtime leg stopped.

- `packages/flowsafe/src/do-runner/remote-step-journal.ts`, `recoverableResults` and `reconcileResult`: the current scan selects retained/resuming work through the v3 partial index, while `reconcileResult` returns `pending-quiescence` for an active `resuming` row. The journal migration must preserve those records and add durable attempt evidence before any automatic reclaim.

- `packages/flowsafe/src/agent-runner/durable-agent-runner.ts`, `FlowsafeDurableAgent.resumeViaRuntime`, and `packages/flowsafe/src/do-runner/runtime.ts`, `RunnerRuntime.resume`: the current call passes no attempt token:

```typescript
const summary = await this.#runtime.resume(
  this.getWorkflow().id,
  options.runId,
  resumeOptions,
);
```

`RunnerRuntime.resume` then enters the pinned engine with `run.resume({ step: options.step, resumeData: options.resumeData, requestContext })`. It has no remote-step attempt generation. Concurrent replay is excluded from this plan.

- `packages/flowsafe/src/do-runner/fenced-workflows-d1.ts`, `FencedWorkflowsStorageD1.persistWorkflowSnapshot`: the current storage seam accepts `PersistInput` without a remote-step attempt generation:

```typescript
override async persistWorkflowSnapshot(args: PersistInput): Promise<void> {
  const scope = this.#scopes.getStore();
```

The installed `node_modules/.pnpm/@mastra+core@1.73.0_express@5.2.1_zod@4.4.3/node_modules/@mastra/core/dist/agent/durable/index.js`, `executeDurableToolCalls`, calls the supplied tool closure:

```javascript
const result = await tool.execute(toolCall.args, {
```

The installed `dist/workflow-event-processor-LHwOlFwA.js` also invokes a tool closure from its workflow tool execution path:

```javascript
return tool.execute(inputData, toolContext);
```

The installed `dist/agent-D-eTzWxF.js` enters a resumed agent run and can resume nested work:

```javascript
const executionPromise = resumeContext ? run.resume({
```

```javascript
else if (resumeNested) res = await run.resume({
```

The pinned engine's workflow writes include persist, replace and patch paths without a remote-step generation. The closure calls above are enough to rule out an effect adapter that wraps only the FlowSafe host route. A future automatic-continuation proposal must inspect all engine write paths and bind its advancing write to durable resume provenance; a separate post-write journal update cannot serve as the receipt.

## Amendment: execute the host-owned notification operation (2026-10-13)

**Goal and status.** Implement recovery of a selected host-owned notification enqueue after a lost acknowledgement or host restart. `D1NotificationsStorage.createNotificationOnce` is the selected executor; it inserts the notification and immutable request receipt in one D1 batch. A native two-Durable-Object contention fixture has proved local atomicity. No production caller or deployed D1 proof exists. This amendment supersedes the earlier statement that no executor is selected; it does not authorize automatic Mastra `run.resume` replay.

### Current baseline and constraints

- Branch: `plan/flowsafe-remote-step-broker`; amendment base: `97237039350137f42bdc603303f3496f3e7f1431` (2026-10-13). Reconfirm each appendix anchor against the checked-out commit before editing. The historical `4096fbd` baseline above remains a record, not an edit target.
- Use the repository's locked Node, pnpm, `@mastra/core@1.73.0`, Cloudflare Workers, Durable Objects and D1. Work in a linked worktree. Put checks and build artifacts under `/tmp`, and record an exit status in each log. ASP signing, dispatch policy and customer UI remain in ASP.
- The 100,000 retained notification receipt cap is approved and implemented per configured D1 table prefix. One D1/prefix per deployment is the supported topology; a multi-prefix deployment must refuse this profile until deployment-wide accounting is designed. Measure D1 `rows_read` and latency near the cap before release; do not silently purge receipts.
- A workflow-target v4 journal and private workflow route exist, but the selected executor and target-thread recovery below apply to an **agent** invocation. The workflow-target continuation remains a separate gate. The existing `recoverableResults` deliberately selects `target_kind = 'agent'` while drain inventory retains both kinds.

### Operation identity and state contract

The host selects the notification profile from trusted configuration, never from a result body. The profile fixes the notification source, kind, recipient thread/resource, identity and payload construction. Name and version one canonical projection, `notification-record-values-v1`, inside `notifications-d1.ts`; both the journal preparation and `createNotificationOnce` use its `JSON.stringify(recordValues(record))` UTF-8 bytes after defaults and field conversion. Persist the exact `CreateNotificationInput`, projection version, projected bytes and their SHA-256 digest before any effect attempt. The executor computes its receipt `request_digest` from those same bytes and returns or exposes that digest for exact comparison with the journal. Unknown projection versions refuse. Its `operationKey` binds deployment ID, journal invocation ID, run start token, suspension identity and the profile's operation ID; it is stable across host restarts and retries. The journal owns the mapping from this identity to one notification ID. A new resume lease owner does not change either ID. The executor's existing digest check refuses a reused key with changed notification input; the journal compares its stored digest to the executor receipt on every retry.

Use a versioned journal record with states for prepared operation, effect admitted, receipt recorded, resume admitted and unresolved continuation. Keep terminal settlement and retained drain inventory. Preparation stores identity and request but grants no effect authority. Immediately before the first executor call, one conditional D1 write admits that exact host operation. Its SQL predicate checks the current fence row, deadline, stored principal and binding, start token, result digest, journal revision and authoritative suspension identity in the same D1 database transaction; the host's preceding run read is diagnostic, not the authority check. Require a schema path that can compare the authoritative snapshot's current suspension and generation in that transaction. If it cannot, leave the profile disabled. A zero-row or unreadable admission result refuses the effect. The successful write is the effect's linearization point: a later deadline or fence change cannot erase an already admitted operation, whose receipt remains drain work. If the admission write's outcome is ambiguous, reread the durable operation state before any executor call; never infer success from a timeout. After `createNotificationOnce` returns, persist its digest-bearing receipt with an equally bound compare-and-swap. A lost response is recoverable by calling the same method with the exact key and input only for a durably admitted operation; it returns the original receipt or refuses a changed request. Add a read-only receipt lookup inside `D1NotificationsStorage` so recovery can observe an old effect without creating a new one. If no receipt exists and admission was refused or cannot be proved, leave the row unresolved. A cap error before insertion leaves an admitted operation visible; existing-key retry still works at the cap.

Persist a **one-time resume-admitted marker** before entering Mastra. It binds the exact retained result, suspension and operation receipt. The journal must permit one transition into admission and refuse every later attempt, including after lease expiry, deploy and restart. A crash after admission but before the call leaves an unresolved continuation; neither the receipt nor an advanced snapshot proves a second `run.resume` safe. The selected notification can recover automatically, but Mastra continuation cannot until its snapshot writes have generation fencing and attempt-linked provenance. This limit is an explicit release behavior, not a pending implementation shortcut.

### Edit anchors and before → after

The following excerpts are verbatim at the amendment base. Method names in the target sketches specify the contract to implement; inspect the actual types and line positions before editing.

1. **Executor:** `packages/flowsafe/src/signals/notifications-d1.ts`, `D1NotificationsStorage.createNotificationOnce` currently enters with:

   ```typescript
   async createNotificationOnce(
     operationKey: string,
     input: CreateNotificationInput & { id: string; createdAt: Date },
   ): Promise<{
     operationKey: string;
     notificationId: string;
     threadId: string;
   }> {
   ```

   The existing D1 batch inserts a receipt first and gates the inbox insert on `changes() = 1`. Reuse this direct method. Extract its `JSON.stringify(recordValues(record))` projection into one versioned function that the trusted host and executor both call, without changing the bytes or receipt behavior. The adapter constructs and freezes arguments from the prepared journal row. Expose the receipt digest and a read-only lookup under the same storage boundary; do not replace the D1 effect transaction.

2. **Journal:** `packages/flowsafe/src/do-runner/remote-step-journal.ts`, `reconcileResult` currently starts:

   ```typescript
   if (invocation.status === 'resuming') return 'pending-quiescence';
   const owner = await this.claimResume(invocationId, 60_000);
   return this.settleAfterResume(invocationId, owner);
   ```

   Keep this fail-closed path for ordinary remote steps. Add a separate notification operation path whose order is `prepare` → `admitEffect` → `createNotificationOnce` → `recordReceipt` → `admitResume` → one trusted runtime call → witnessed settlement. `admitEffect` owns the atomic authority predicate described above. Recovery may repeat the executor with the stored key and request after proved effect admission and before `admitResume`; it must never use `claimResume` to reclaim a previously admitted Mastra call.

3. **Host:** `packages/flowsafe/src/agent-host/thread-host.ts`, the retained-result route currently enters Mastra after:

   ```typescript
   const leaseOwner = await reader.claimResume(invocationId, 60_000);
   const resumed = await withExecution(execution, () =>
     durable.resumeViaRuntime({
   ```

   A notification-profile branch runs inside the same target thread's `withDispatchLock`. It re-reads the authoritative run and stored principal/binding, then requires the conditional `admitEffect` D1 write before calling `createNotificationOnce` from trusted host configuration. It records the receipt, rechecks authority for continuation, then commits the one-time marker before this runtime call. The ordinary branch retains its current fail-closed behavior. A host-only recovery method uses the same lock, authentication and stored-principal checks; it exposes no public result route.

4. **Maintenance:** `createThreadAgentHost.recoverOwnership` currently uses:

   ```typescript
   return withDispatchLock(async () => {
     const storage = options.stateStorage();
   ```

   The recovery method shares this serialization point. A scheduled maintenance Worker advances a durable, bounded D1 cursor over active notification-profile operations and routes each selected item to its owning thread Durable Object. The thread method shares the existing dispatch lock. Its alarm owner also wakes pending local work during no traffic; extend its existing `recoverOwnership` scheduling and delete conditions so notification work cannot be erased by owner recovery. Process a fixed batch, persist the next cursor, wrap at the end and rearm while work remains. Do not install a second independent alarm owner or scan retained terminal history. A failed item stays visible; one item cannot prevent progress through later IDs. A lease expiry schedules inspection but never authorizes another Mastra call.

5. **Migration and mixed writers:** `remote-step-journal.ts` currently recognizes version 4 with:

   ```typescript
   if (record(rows[0])?.id === 1 && record(rows[0])?.version === 4) {
   ```

   Its current update trigger is:

   ```typescript
   const REMOTE_STEP_WRITE_TRIGGER_SQL = `CREATE TRIGGER ${REMOTE_STEP_WRITE_TRIGGER} BEFORE UPDATE ON ${REMOTE_STEP_TABLE} WHEN NEW.write_revision <= OLD.write_revision BEGIN SELECT RAISE(ABORT, 'remote step writer version changed'); END`;
   ```

   Migrate known v1–v4 rows into a validated v5 layout without losing permits, retained results or unsettled obligations. Add a required `write_generation` initialized to a positive value for migrated rows. Require v5 inserts to supply it and v5 updates to increase it through a D1 trigger; an already-initialized v4 writer preserves the value and is refused. Validate the table, indexes, trigger and exact schema version before reads or writes. Unknown or malformed layouts refuse. An old Worker that initializes after migration sees unknown version; prove both old-writer cases in an old/new Worker exercise before enabling dispatch. Legacy `resuming` rows without the new admission evidence remain unresolved.

### Ordered implementation checkpoints

1. **Journal contract and migration:** Add the v5 operation fields, conditional effect-admission transition, active index and drain inventory projection. Preserve existing v4 behavior and data. Refuse stale writers with the generation trigger and test both old-initialized and fresh old versions. Prove that the admission SQL can compare current fence and authoritative suspension in the same D1 transaction; otherwise keep the profile disabled. Use `remote-step-journal.ts` migration and conditional-write patterns rather than a parallel journal.
2. **Trusted host executor:** Add the closed notification profile to the target thread host. Derive all effect fields from trusted stored context, share the executor's versioned projection and use `D1NotificationsStorage.createNotificationOnce` directly after proved effect admission. Save its digest-bearing receipt. Add the single-admission transition before runtime entry. Keep `resumeViaRuntime` one-shot and report ambiguous outcomes as unresolved.
3. **Recovery duty:** Add host-only inspection, a scheduled D1 cursor scan and target-thread alarm wake through the existing owner recovery scheduler. Repeat a previously admitted effect with its stable key; after a refused or ambiguous admission, use read-only receipt inspection and keep missing evidence unresolved. Preserve progress after restart, duplicated alarms and an item that repeatedly fails. Never retry admitted continuation.
4. **Cutover proof:** Keep the profile default off until native fresh-isolate and deployed D1 proof, old/new Worker refusal, authenticated host route negatives, cap latency measurement and drain inventory checks pass. Review and release via the Anchorage `dev` process; ASP integration follows a published, release-age-eligible FlowSafe version.

No new package is needed. The existing direct D1 notification storage is the smallest integration boundary; a new executor package would duplicate its atomic receipt contract. An external notifier lacks this repository's D1 transaction and cannot satisfy this plan without a separate proof. General Mastra replay, workflow-target continuation, arbitrary tools, payment, customer dispatch and ASP policy are excluded for the reasons above.

### Scoped verification and proof

Select tests from the checkpoint's changed paths and shared contracts. The v5 schema is a shared contract, so run the owning FlowSafe package suite once for that checkpoint under the quality-gate exception. For other checkpoints use the named files below, then changed-file Biome, package typecheck/build when the tool cannot retain config with file targets. Redirect each command to a distinct `/tmp` log with the content ID and `EXIT_STATUS` footer. Run the repository full suite once after the final checkpoint, not at intermediate checkpoints.

```bash
pnpm --filter @proofoftech/flowsafe exec vitest run \
  src/do-runner/remote-step-journal.test.ts \
  src/agent-host/thread-host.test.ts \
  src/signals/notifications-d1.test.ts
pnpm exec biome check packages/flowsafe/src/do-runner/remote-step-journal.ts \
  packages/flowsafe/src/agent-host/thread-host.ts \
  packages/flowsafe/src/signals/notifications-d1.ts
pnpm --filter @proofoftech/flowsafe typecheck:test
pnpm --filter @proofoftech/flowsafe build
pnpm --filter @proofoftech/flowsafe spike:verify
```

The gate owner narrows these candidates to the changed-file list and records the exact commands in its logs. All implementation checkpoints are T2: independent clean-code, QA, architecture and security lanes review the checkpoint diff and the called journal, host, D1 notification and alarm contracts. Review this amendment once at design level before implementation.

The live proof must show one D1 notification and receipt when two fresh object instances race, including a lost acknowledgement, same-key retry at the last receipt slot, changed-request refusal and crash before/after receipt. Change the fence, deadline and suspension after preparation but before effect admission: the D1 write must refuse and the inbox must stay empty. Interrupt the admission response: recovery must read its durable result before any effect. Compare the journal's projected request bytes and digest with the executor's receipt digest, including defaulted and date fields. The proof must also show a persisted cursor eventually reaching work beyond one batch, duplicate alarm delivery, restart and no-traffic wake, an old Worker unable to write v5 rows, and blocked drain while Mastra admission is uncertain. Inspect the authoritative D1 receipt and inbox after each fault. Local workerd evidence does not substitute for a deployed D1 exercise. Successful notification recovery never implies automatic continuation; a test that calls `run.resume` twice fails this plan even if it observes one notification.
