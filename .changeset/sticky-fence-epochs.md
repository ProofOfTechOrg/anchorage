---
'@proofoftech/flowsafe': minor
---

Add versioned execution-fence administration with artifact epochs, a sticky epoch requirement, transition revisions and exact last-command retry receipts. Upgrade supported legacy schemas additively without changing existing state or timestamps. Missing rows in new-format schemas fail closed. Admin responses omit receipts, proof execution identity and tokens; legacy commands remain compatible only while the epoch requirement is optional.

Activate v2 Runtime generations with independently generated execution tokens and preserved original principal, logical target, caller epoch and agent mode across resume legs. Fenced starts require the actual D1 domain's positive initial-write witness before engine entry, binding the winning reservation and proof in the same admission transaction. Capable D1 without a fence keeps its actual namespace and ordinary persistence options; custom storage explicitly asserts no D1 namespace. Unfenced keyed starts bind their prepared identity before creation and retain uncertain outcomes.

Persist preparing, prepared and prepared-unfenced journals in both managed hosts. Recover exact owned initial generations through the existing raw-row conditional repair without replaying effects or deleting tokenless snapshots. Require strict terminal reservation settlement before approval, dispatch, owner and lifecycle cleanup, then clear only the matching journal. Cold agent alarms initialize actual wrappers with the verified instance scope. Legacy journals and uncertain unfenced pending/absent outcomes remain unresolved.

Keep the thread blocking-run check and run-record installation under the same lock. Validate workflow journals against the owning object's address and recheck complete agent journals after recovery waits before releasing reservations. Keyed recovery requires its configured reservation store before bookkeeping, including nonterminal outcomes.

Share cold agent-wrapper initialization across concurrent requests. Probe the owning execution's liveness before reclaiming an existing reserved key, so a stream awaiting Core cleanup keeps retries pending without stranding the key. Unreadable liveness replies refuse the retry instead of authorizing a claim.

Require a matching nonpending durable observation before modern start/resume success or the agent persistence acknowledgement. Return `RUN_START_PENDING` for a valid initial generation, preserving journals, watchdogs and pending schedule/deadline budgets. Project root-local summaries from the same selected observation, retaining detailed nested resume preparation and legacy compatibility.

Preserve valid v1 and absent-provenance ordinary status, resume and lifecycle completion through one authoritative observation. Apply the existing binding, canonical-record and principal checks to legacy status. Legacy terminal cleanup requires no recovery journal and a confirmed raw terminal outcome; it never manufactures generation identity or spends a start key. Normal termination retains canonical agent records until lifecycle completion confirms.

Create modern-unbound reservations and replace run-only claim, release and settlement with exact observed-row operations. Claim/release stamps advance without serving as generation tokens; only the caller's own valid write result proves a winning claim. Private replay compares the full generation before pending/result classification and preserves the value from its one authoritative read. Alias binding and prepared binding retain their distinct response-loss rules. Remove `claim`, `release`, `settleRun` and `rollbackFencedStart`; custom router wiring must provide the private `persistedStart` callback.

Guard replay proof nomination with its original proof round/caller epoch, current exact snapshot and bound reservation at the final SQL write. Legacy proof setters cannot overwrite modern identity. Runtime, workflow-host, approval and signal re-entry gates compare complete physical generations and retain their original expectation through relevant waits. Direct approval compositions now pass an explicit trusted workflow namespace. External effects are not transactional with these checks.

Capture trusted authority before asynchronous work across Worker configuration, protected JSON/header transport and the eighth agent-start authority argument. Keep public bodies and application context from supplying a winning claim. Preserve source-owner versus initiating-principal attribution, exact lifecycle counter exhaustion checks and rejection of sparse economic-operation lists.

Final schedule-write protection, generation-aware retention and actual workerd/D1 acceptance remain required before enabling artifact-epoch enforcement across a deployment. Administrative support and explicitly unfenced execution do not supply those guarantees.
