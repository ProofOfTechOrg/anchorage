# @proofoftech/fleet-control

## 0.5.0

### Minor Changes

- 8ae28bf: Add token-driven bounded no-export cleanup with durable operation-keyed terminal receipts. `advanceCleanupDeployment()` performs at most one bounded scan chunk or one action group per call; the terminal call persists an immutable receipt, releases the deployment's ownership claims, and deletes the fleet row in one D1 batch. Receipts survive same-key reprovisioning and force decommission; read them with `readCleanupReceipt()` and garbage-collect them explicitly with `pruneCleanupReceipts()` (database-time cutoff, stable order, limit 1..1,000). `cleanupDeploymentArtifacts()` and the default failed-provision rollback drain this engine on capable stacks.

  - **BEHAVIOR CHANGE:** No-export cleanup is narrowed to deployments that provably never authorized a candidate invocation. New records persist an invocation-authority carrier on their first durable write, and every candidate-invoking dispatch (external candidate upload, first maintenance request, version override, promotion) commits an authorization timestamp durably before the provider call. Authorized rows, legacy carrier-less rows at `application-resources-deployed` through `maintenance-armed`, and rows with external staging evidence now refuse toward export-backed decommissioning; trusted plain deployments keep no-export cleanup through `worker-deployed`.
  - **BEHAVIOR CHANGE:** Workers for Platforms and external-artifact deployments always refuse no-export cleanup: every current candidate binds the deployment D1, and no reviewed no-data profile exists.
  - **BEHAVIOR CHANGE:** A failed provision whose rollback admitted the bounded engine is durably `cleanup-advancing`. `provisionDeployment()` refuses to resume it with a fixed redirect to cleanup; complete the cleanup (receipt) and reprovision fresh. Previously the row kept its provisioning phase and could be retried as provisioning.
  - **BEHAVIOR CHANGE:** External-candidate and WFP failed-provision rollback no longer tears the deployment down. The engine refuses before any mutation, the row keeps its phase and stays provisioning-retryable, and teardown routes to export-backed decommissioning.
  - **BEHAVIOR CHANGE:** Cleanup completion releases the deployment's ownership claims; decommission claim behavior is unchanged. Force decommission releases current claims on capable stores, refuses during an active bounded cleanup, and on legacy lease implementations without `deleteReleasingClaims` deletes the row and leaves claims for later reconciliation. Force does not delete the ordinary Worker script or application R2, so do not reprovision the same names until residual physical resources are confirmed removed; provisioning fails closed on ownership mismatch.
  - **BEHAVIOR CHANGE:** `auditFleetDrift()` treats a deployment under active bounded cleanup as its own reconciliation authority: no expectation-based, orphan, or record-level findings (including `incomplete-provisioning`) while the cleanup intent is active.

  Add `ProvisionDeploymentOptions.failureCleanup: 'drain' | 'bounded'` (default `'drain'`); with `'bounded'` the rollback performs at most one bounded advance and surfaces the resumable outcome through the new `ProvisioningError.cleanup` field.

- 36a4b7c: Add token-driven bounded normal decommissioning for at-least-once control-plane Worker workflows. Fleet D1 owns scan progress. Each call performs at most one bounded scan chunk; only an exact matching verify may immediately consume that result through its single same-lease resource action. Other calls perform at most one lifecycle or resource action group.

  Persist an immutable database-export receipt authority before the first D1 scan or export. Retries after artifact commit or Fleet state-write loss converge on the same filesystem or R2 receipt; authority changes and byte collisions preserve the committed winner and fail closed. Custom bounded backends must expose the paired receipt authority and export capability. Queue-driven bounded decommissioning requires Workers Paid because its bounded multi-R2 read groups can exceed the Free plan external-subrequest limit.

  Add a root-only bounded backend-switch advance API that uses the same durable token and receipt guarantees. It binds teardown to one immutable switch snapshot and captured entry subphase, advances at most one release, R2 resource, scan chunk, or D1 action group per call, and preserves legacy recovery after a shell-less deployment reaches export authorization. A pending ordinary Worker requires lossless exact-version inspection, authoritative secret-name inventory, and its persisted Durable Object namespace identities. Custom switch providers must expose the bounded scan, receipt, database, residual, delete, and conditional pending-artifact inspection capabilities required by the durable state they resume.

- d9f864f: Add a bounded, resumable fleet drift audit API with a durable, provider-neutral operation store. `advanceFleetAudit()` performs at most one bounded stage chunk per call — one global-stage slice of up to `maxItemsPerCall` items (1..2,000, default 500), or exactly one Fleet record's inspection and re-arm — against a `FleetOperationStore`; `D1FleetOperationStore` implements that port over the existing Fleet D1 binding with account-and-kind-scoped leases, lease-fenced guarded batches, and audit generation pinning. Call `start` with an operation id, the audited records, and `staleAfterMs`, then re-enqueue only the pending token each call returns. Start requires at most 10,000 records whose canonical bytes total at most 16 MiB, with each record within the 96 KiB staged-row byte bound and the per-record structure bounds: plain JSON data (no `undefined`-valued properties, dates, class instances, or cycles) within depth 64, 8,192 nodes, and 4 KiB per string value or object key. Every record must satisfy the deployment identifier grammar, and an explicit generation must be a positive safe integer. Every such refusal has a fixed message and precedes every durable effect. The `staleAfterMs` and operation-id refusals also occur before the lease; after the lease row is written, the foreign-kind, no-finalized-generation, and `auditClock`-sample refusals write nothing else. These accepted inputs are intentionally narrower than `auditFleetDrift()`, which does not require that identifier grammar, an explicit generation, or the bounded path's row and structure bounds. Read the findings back page by page with `readFleetAuditFindingsPage()` once the operation is terminal. Each page comes back in ordinal order whatever order the store's page arrived in and carries `nextAfterOrdinal`, the cursor to pass back as `afterOrdinal` on the next call — absent only on an empty page, which is legal only when `done` is set. The reader verifies the page rather than trusting it: a page that is empty while unfinished, or that is not the contiguous ordinal run following the cursor, refuses with `fleet operation state is malformed`. `abandonFleetAuditOperation()` unblocks a stuck running operation and releases any pin an already-terminal one still holds.

  - `auditFleetDrift()` keeps its exact signature, refusal message, finding vocabulary, finding order, provider interaction order, return value, and stop behavior. It now drains the same decomposed stages in memory, and a frozen golden baseline (findings and the full store/backend/resolver call log) pins all of that.
  - **HARDENING:** the bounded engine never persists the raw diagnostic bytes a one-shot audit composes call-locally. The three resolvers, the inspection, the re-arm, and the segmented multi-duty `maintenance-stale` composition durably record a fixed template alone; any finding detail, composed by the engine or passed through from the pinned inventory generation, that fails a non-throwing credential-substring and control-byte gate persists a fixed withheld-detail fallback instead of aborting the operation.
  - An audit start pins exactly one finalized `@proofoftech/fleet-control` R3 inventory generation and keeps it through completion, so a finding page stays interpretable against the exact generation it was computed from; only explicit result garbage collection, terminal failure, or abandonment releases it. A replayed start never re-resolves "latest": it reuses the persisted generation.
  - Every finding or fact must fit the staged-row envelope: a 16 KiB JSON-serialized payload, 4 KiB per string, and the codec's depth and node bounds. The coordinator detects an excess before the store sees the row, fails the whole operation with the durable `emission-bound-exceeded` reason, and releases the pin. One record's whole per-call emission set (its findings plus the cross-record ownership facts it newly claims) must also fit inside the one guarded D1 batch its `per-record` call commits — a ceiling of 99 rows: 99 emitted rows plus the one run-record update is exactly the 100-statement budget and is accepted, while 100 or more emitted rows fail. A record whose live inspection alone would emit 100 rows fails with the same reason rather than emitting a partial finding set.
  - The bounded path differs from the drain in exactly four classes: every resolver, inspection, and re-arm failure and every multi-duty `maintenance-stale` finding persists one of the six fixed detail-template families where the drain composes the raw diagnostic; any unsafe finding detail, composed or passed through, becomes the fixed withheld-detail fallback; concurrent mutation can cause either a re-arm refusal on a Fleet reread mismatch or inspection-derived findings against later provider truth, with the bounded path's typically older snapshot making both more likely; and `emission-bound-exceeded` (from the staged-row envelope or the 99-row ceiling) and `generation-unavailable` are terminal whole-operation failures with no drain counterpart, while `auditFleetDrift()` completes and returns its full finding array over the identical world and clocks. Every other output is proven byte-for-byte equivalent to the drain under identical frozen worlds and clocks. The four-class claim also assumes `backendFor`, `specFor`, and `maintenanceSecretFor` are functions of record _value_: the bounded path hands them canonical snapshots rebuilt from the staged rows, never the caller's own objects, so an identity- or prototype-keyed resolver diverges from the drain in a fifth way this list does not cover.
  - Per-call cost is not free: every advance call that runs a stage chunk re-reads the pinned generation in full and re-pages the accumulated `record` rows; a record-processing `per-record` call additionally re-pages the accumulated `fact` rows. Each stage-running call also structurally re-parses every accumulated `record` row through three plain-data traversals. That three is this coordinator's own ingress only: against the D1 store each of those same rows additionally costs a `JSON.parse` of the stored payload and the staged-row codec's own bounded-plain pass, so the real per-row constant factor is higher than three. `finding` rows are never re-paged. A stale token, a `start`, and a `finalize` call read neither.
  - Aggregate cost: one bounded audit spans `1 + records + Σ max(1, ⌈stage_i / maxItemsPerCall⌉)` stage-running calls, for a `maxItemsPerCall` held constant across the operation — the option is per-call, so varying it between calls changes the count: one per-record-to-finalize transition, one processing call per record, and at least one call per global stage. The sum runs over the eleven global stages rather than over distinct sources: `deployment-gaps`, `namespace-expectations`, and `r2-expected` each chunk the audited-record array independently, so that one array is walked by three separate stage runs. Every such call re-reads O(G) generation rows and re-pages and structurally re-parses O(R) accumulated `record` rows. In the records-dominated case, this is O(records) full generation re-reads, O(records²/1,000) accumulated-row page reads, O(records²) billed rows read, and O(records²) structural `FleetRecord` re-parses at three plain-data traversals each, the dominant CPU term. This checkpoint's in-memory suite measured roughly 0.25 ms per record-row re-parse and roughly 0.5 s for one `per-record` call over 1,001 accumulated rows. Multiplying the first figure by that row count accounts for about half the second; the remainder is the call's fixed cost: the pinned-generation re-read, both row pagings, and the record's own provider step. Read the per-row rate as the 0.25-to-0.5 ms band those two figures bracket rather than as a single constant, and as an order of magnitude from in-memory fakes rather than a production measurement. A late per-record call at the 10,000-record ceiling therefore spends seconds of isolate CPU re-parsing before its provider work. The per-call guarantee covers bounded provider work and bounded emission, not bounded CPU or bounded rows read. See the fleet control guide for the full envelope.

  The operation store rejects invalid row-page selectors before schema work. Invalid public selectors use input-specific errors; malformed durable records retain `FleetOperationStateError`.

  `stageRows` rejects conflicting immutable staged payloads within its current batch. `commitProgress` refuses conflicting immutable payloads and missing item-update targets without advancing progress or retaining sibling mutations from that batch. Exact restaging remains idempotent. Replaying an uncertain commit requires the same intended run record and row payloads; composing a new transition requires reading persisted progress first. Database errors propagate to the trusted caller. Lease expiry can still leave earlier staged rows while the progress update refuses, so immutable retry payloads remain a caller obligation.

  `failOperation` accepts at most one item update. Terminal transitions refuse operation IDs belonging to another operation kind. Row-watermark refusals preserve sibling rows, and noncontiguous caller inserts below a claimed watermark refuse before SQL. See `FleetOperationLease` for the operation-store contract.

  The new `readFleetAuditFindingsPage()` resolves to the exported `FleetAuditFindingsPage` type, a `done`-discriminated result: `{findings, done: true, nextAfterOrdinal?}` or `{findings, done: false, nextAfterOrdinal}`. No existing public export changes shape, and the Worker subpath is unchanged.

- 2600c07: Add a bounded, resumable account inventory API with durable generations. `advanceFleetInventory()` performs at most one provider stage chunk per call against a `FleetInventoryRunStore`; `D1FleetInventoryRunStore` implements that port over the existing Fleet D1 binding with operation-keyed runs, lease-fenced guarded batches, generation pinning, and bounded garbage collection. Build the provider seam with `cloudflareFleetInventoryContext(client)`, call `start` with an operation id, then re-enqueue only the pending token each call returns. The final call returns a `FleetInventoryGenerationRef`; read the rows back as today's `FleetResourceInventory` with `readFleetInventoryGeneration()`. Budgets are caller-supplied and validated: `maxProviderRequests` 9..1,000 and `maxStagedRowsPerChunk` 1..2,000 (default 500).

  Starts reject malformed identity and inconsistent options before claiming durable run state. Chunk commits retain the operation's account, physical generation and options. Conflicting immutable row or fact payloads roll back sibling writes; duplicate keys refuse before SQL. Exact replay compares the intended run record as well as staged bytes, so a failed run at the same revision cannot be returned as a successful chunk. Database errors propagate, and callers must retain immutable payloads across uncertain responses and lease-expiry retries.

  Cross-account operation-ID collisions roll back the losing head claim and generation allocation. Exact failure and finalized continuation retries repair interrupted head cleanup while preserving newer operations and historical pin requirements. Future tokens refuse before fallback repair. Pruning retains active terminal generations until head cleanup finishes. Pin admission verifies the finalized physical target in its INSERT, preventing orphan pins when reclamation wins a concurrent call. Pin admission also checks retained row/fact manifests, so partial reclamation cannot create a pin over missing data. Finalization and pinning require dense row ordinals.

  - `collectFleetInventory()` keeps its exact signature, refusal message, provider encounter order, finding vocabulary, finding order, and result bytes. It now drains the same engine in memory, and a frozen golden baseline pins all of that. The one exception is the scale limit below.
  - **BEHAVIOR CHANGE (scale limit):** `collectFleetInventory()` is now subject to the same `maxProviderRequests` bound as a bounded run, capped at 1,000 per stage chunk, and six stages carry no resumption cursor so they must finish in one chunk. An account whose largest such stage needs more than 1,000 provider operations — in practice roughly 1,000 prefix-matching plain Workers, which `route-claims` reaches first — now rejects with `fleet inventory stage '<stage>' cannot complete one chunk within its provider request budget` instead of returning an inventory, where the previous single-pass enumeration completed under the 10,000-item collection bound. Nothing is written and no partial result is returned. There is deliberately no unbounded mode; narrow `scriptNamePrefix` to split such an account. See the fleet control guide for the stage list and the arithmetic.
  - Its options parameter gains the exported alias `CollectFleetInventoryOptions`. The shape is identical, so this is not a break.
  - **HARDENING:** the two durable finding details that previously interpolated a provider error string now store the fixed templates `registered script '<name>' could not be inspected` and `plain Worker '<name>' could not be inventoried`. The transient text stays call-local, so `collectFleetInventory()` still returns today's exact bytes while a persisted row carries no provider text.
  - **HARDENING:** a raw host-routing KV key name that is over-length or credential-shaped refuses the run; one that is merely unprintable or base64-shaped yields a `malformed-script-registration` finding naming the key by its listing ordinal rather than by its bytes. That finding is positionally attributable but does not carry the offending name.
  - Only a finalized generation is readable. The latest finalized generation reads without a pin; every older generation must be pinned before it is read, because pruning removes finalized-or-failed, non-latest, unpinned generations.
  - A generation is a point-in-time-per-stage snapshot, not a globally consistent one. A resource that changes between stages is recorded exactly as the single-call enumeration surfaces it — the same guarantee `collectFleetInventory()` has always given.
  - Bounded cursor history beyond the last committed offset is deliberately out of scope: a single-pass resumable scan needs only the last offset.
  - **INTERNAL:** `inventoryBoundExceeded` is consolidated into `cloudflare-client-config.ts` and shared by both provider modules. The refusal messages are byte-identical.

  No existing public export changes shape, and the Worker subpath is unchanged.

- e3edf87: Add durable fleet upgrades through `advanceFleetMigration()`, with a frozen target digest and per-item plan, one admission or plan step per call, first-error stopping, item paging and explicit abandonment. The bounded API requires a `FleetOperationStore` alongside the existing deployment store; custom deployment stores can continue using the one-call `migrateFleet()` drain.

  Fleet and provider state remain mutation authority. Continuations re-read that state under per-call deployment leases and recover cursor-loss windows through the shared migration engine. Public documentation describes accepted inter-call races, strict fresh-admission limits after Durable Object tag movement, at-least-once settlement, operation-store convergence residuals, and the repeated resolver/provider and whole-item-read costs. One step is not a fixed provider-request, CPU or row-read budget.

  The existing drain retains its recorded ordering and behavior apart from the separately documented correction to target application-binding validation during plain Worker upgrades.

- bb9291c: Add `CloudflareApiPlainWorkerBackend`, a direct Cloudflare API backend for platform-authored ordinary Workers. Its adapter classifies each mutation's dispatch under the operation's own execution context, so a queued mutation's pre-dispatch failure rejects instead of resolving `{ status: 'failed' }`. Construct it with a plain-only `CloudflareProvisioningClient`. Configure that client with a shared rate coordinator and a durable export store. The existing Wrangler and Workers for Platforms backends keep their public provisioning contracts.

  Expose the configured provider request timeout through the public `CloudflareProvisioningClient.requestTimeoutMs` getter.

  Harden provider behavior across the built-in backends:

  - **BEHAVIOR CHANGE:** Disable Cloudflare SDK logging even when `CLOUDFLARE_LOG` requests verbose output.
  - **BEHAVIOR CHANGE:** Bound every paginated Cloudflare inventory and fail instead of truncating an over-bound result.
  - **BEHAVIOR CHANGE:** Disable SDK retries for Worker upload and deployment, D1 creation and query, and each D1 export poll. This includes the D1 query path shared by both provider backends.
  - **BEHAVIOR CHANGE:** Redact signed URLs, provider response details, headers, and original causes from database export failures.
  - **BEHAVIOR CHANGE:** Replace uploaded secret plaintext in **ordinary-Worker** provider error messages, and in the cause chain of transport failures behind them, before returning a failed mutation outcome.
  - **BEHAVIOR CHANGE:** Surface a lost lease before database or R2 reconciliation in all three affected create paths.
  - **BEHAVIOR CHANGE:** Reject `PlainWorkerBackend.identityCaller` values that are not printable, single-line ASCII tokens from 1 through 128 characters.
  - **BEHAVIOR CHANGE:** Refuse a reconciled Worker upload whose workers.dev or preview-URL state does not match the intent instead of accepting it by tag rediscovery.
  - **BEHAVIOR CHANGE:** Queued provider mutations assert their own lease. Under concurrency pressure a queued mutation previously ran under the preceding operation's execution context.

  When upgrading, construct direct ordinary-Worker clients with `plane: 'plain-worker'` and no `dispatchNamespace`. Keep `dispatchNamespace` on Workers for Platforms clients, provide one quota coordinator across every replica sharing a provider token, and ensure the token can complete the documented attachment scans before destructive teardown.

- 308bd39: Expose `@proofoftech/fleet-control/cloudflare-control-plane` for a dedicated trusted Cloudflare Worker. The factory composes direct Cloudflare operations with durable Fleet D1 state, shared D1 quota coordination, and private R2 database exports. Continuation tokens resume bounded lifecycle operations against stored authority.

  Install the required `@cloudflare/workers-types >=5.20260730.1 <6` peer when typechecking consumers. Keep the Cloudflare token and control-plane bindings outside tenant-serving Workers, and authenticate and authorize operations before calling the library.

  The packed verification uses an unminified namespace-import Worker with raw and gzip regression budgets of 4,128,768 and 589,824 bytes. Those budgets apply 25% headroom to the initial packed measurements, rounded up to 64 KiB. They are repository regression limits; size inventory and audit workloads for the documented Worker resource envelope.

- a534f63: `advanceFleetMigration()` accepts an `AbortSignal` and a completion callback, matching the audit advance. `signal` is call-local and never persisted: it is checked at the public entry, before the action branch, and again at the head of each item advance, outside the step's failure handler — so a cancellation leaves the operation and its items exactly as they were and a later continue resumes, rather than durably failing the item.

  `onComplete` runs on every call that returns `complete` — the call that finalizes the operation, a later continue on the finalized operation, and a replayed start of the same operation id — after the finalization is durable and before that call returns. Delivery is therefore at least once, and the host deduplicates on `operationId`; the alternative that fires only on the running-to-finalized transition loses the notification when the process dies between the durable finalize and the callback. A rejection propagates to the caller and leaves the durable finalization intact.

  `CloudflareAdvanceFleetMigrationOptions` carries both, forwarding `signal` unbound and `onComplete` bound to the caller's options object. Both additions are optional members, so existing callers are unchanged.

- 224421b: `PlainWorkerProvisioningApi.listDatabases` accepts an optional name filter. The direct Cloudflare API adapter forwards it as the D1 list query and the Wrangler adapter filters its parsed inventory locally; `PlainWorkerBackend.findDatabase` passes the deployment's database name and keeps its exact-name comparison and its duplicate-name and missing-UUID refusals.
- cfcd24c: Export the shared `PlainWorkerBackend`, its `PlainWorkerProvisioningApi` port, and the port's ordinary-Worker record types. Port adapters must verify database exports independently against the durable store's committed size and digest. `WranglerLoopBackend` now extends this core while retaining the same constructor options and provisioning members.

  Harden ordinary-Worker provisioning and teardown:

  - **BEHAVIOR CHANGE:** Surface a lost external mutation lease as a failure instead of masking it behind a post-dispatch readback.
  - Preserve both operation and scratch-cleanup failures without masking either.
  - **BEHAVIOR CHANGE:** Refuse D1 bindings and database inventory entries with an empty primary identifier instead of accepting a fallback field or malformed inventory.
  - **BEHAVIOR CHANGE:** Reject malformed Worker version inventory that omits an identifier instead of treating the entry as provider absence.
  - **BEHAVIOR CHANGE:** Keep lease denials distinct from provider absence during Worker deletion.
  - Allocate adapter-owned upload scratch only when an upload is required.
  - **BEHAVIOR CHANGE:** Classify post-install scratch-cleanup failures so callers remove only Workers created by the failed attempt.

  Provider-neutral error messages now describe plain-Worker and provider operations instead of Wrangler. Error-message text compatibility is not claimed by this release.

  When upgrading, consumers that matched `deployWorker` rejections by identity or message text should catch `WorkerDeploymentError` and read `createdByAttempt` and `resourceState`.

- 3c13571: Export `plainWorkerIngressModule` from the root entry for tools that inspect ordinary Worker ingress or calculate the complete upload size. The backend appends this generated module during upload.
- ce0913c: Admit a retired terminal `decommissioned` record as an absent prior in `provisionDeployment()`, so a host can reprovision a decommissioned slug without first clearing its ledger row. A stored row qualifies when it carries the record a completed decommission leaves — its `applicationResources` entries `deleted`, its database export location, digest, and byte count recorded, and no pending lifecycle field — and no unfinished decommission, cleanup, or backend-switch operation; the read is normalized once, before the lifecycle guards, the immutable-mapping asserts, the phase refusal, and the reservation-ownership flag that drives the failed-provision unwind. Force-then-provision continues to work unchanged: `forceDecommissionDeployment()` still removes a terminal row, and a provision over the empty key follows the same fresh path it always did.

  - **BEHAVIOR CHANGE:** A retired terminal record no longer refuses with `cannot be provisioned from phase 'decommissioned'`. Provisioning replaces the row from the supplied `DeploymentSpec`: the slug, logical script name, database name, and route hostname come from that specification, equal to the retired row's when the specification retains them, while the database and its provider-minted ID, the seeded deployment identity, the application R2 resources, and the artifact version are new. The replacement carries no export location, digest, or byte count. A changed specification is accepted, because the immutable-mapping and digest guards read a prior and a retired record is not one; `previousDurableObjectTag` is refused as it is for any other new deployment.
  - **BEHAVIOR CHANGE:** `decommissionDeployment()` replays a terminal record's `DatabaseExport` for a late retry while that terminal row is the stored row. Once a re-provision has replaced the row and the replacement reaches `ready`, a same-spec `decommissionDeployment()` call is a new decommission of the replacement and proceeds against it: the one-call facade carries no operation identity that separates a retry from a new request. Read the export from its retained location before reprovisioning the name; carry an in-flight decommission through `advanceDecommissionDeployment()`, whose token carries the operation identity; or confirm the row's deployment identity — its database ID — before issuing a one-call decommission after a re-provision.
  - **BEHAVIOR CHANGE:** A terminal row that retains an application resource, or whose teardown evidence is incomplete — a force-produced row records no database export — refuses with a message that names that residue and directs to `forceDecommissionDeployment()` after the residual physical resources are confirmed removed, in place of the generic phase message. An unfinished decommission, cleanup, or backend-switch operation refuses through the guard that owns it, as it does from any other lifecycle entry. Every other non-resumable phase keeps the generic refusal.
  - **BEHAVIOR CHANGE:** Over a retired terminal record, the reserved-name database check runs before the first `lease.put`, so a re-provision that refuses a pre-existing database leaves that record — including its export location, digest, and byte count — byte-identical, and the failed-provision unwind no longer deletes a row the attempt never claimed. A provision with no stored row keeps its original order, where the durable reservation is written first.
  - **BEHAVIOR CHANGE:** `auditFleetDrift()` no longer reports `incomplete-provisioning` for a terminal `decommissioned` record. The phase is retained state rather than a phase that advances, so the staleness check misclassified intended retained state as incomplete provisioning.

### Patch Changes

- f100ab3: Harden account-wide D1 and R2 attachment scans with a request-bounded, page-independent resumable engine. Rechecked inventory drift, malformed provider metadata, non-string or repeated dispatch cursors, and page or item overflows now fail closed instead of allowing an incomplete absence proof.
- f593e66: Refuse incomplete Worker binding metadata during D1 and R2 attachment scans so cleanup cannot mistake an unreadable attachment for an unused resource.
- ceab639: Synchronize the parent directories of filesystem exports before returning a durable location, including when an earlier attempt left a newly created directory behind.

  Cancel abandoned export streams when filesystem setup or a supplied export store fails.

- 621fda7: Refuse redirects on the three credentialed provider transports. `CloudflareProvisioningClient`, `PlainWorkerBackend`, and `WorkersForPlatformsBackend` force `redirect: 'manual'` after a caller's `init`, so a bearer credential is not carried to an address the control plane did not choose. The raw dispatch script page and both Workers for Platforms maintenance calls, which read a response the transport does not otherwise classify, throw `CredentialedRedirectRefusedError` on a 301, 302, 303, 307, or 308 and cancel the unconsumed body; the message names the operation and the status, never the address. An SDK-routed redirect surfaces as an `APIError` with the original status, unretried and not classified transient.

  A host whose injected fetch follows redirects itself is unaffected: supply that fetch only from trusted control-plane code.

- 7a446d2: Accept successful Cloudflare list responses with `errors: null` or `result_info: null`. Retry plain-Worker maintenance requests answered by a workers.dev platform 404 or 500 text page, for up to 60 seconds at 2-second intervals by default. Both `PlainWorkerBackendOptions` and `CloudflareApiPlainWorkerBackendOptions` expose `maintenanceRouteReadyTimeoutMs` and `maintenanceRouteReadyIntervalMs` to configure these bounds, and `wait` to delay reconciled mutation retries. Host Workers that fetch tenant maintenance origins on the same account's workers.dev subdomain require the `global_fetch_strictly_public` compatibility flag. Verify account-owned API tokens through the account endpoint, with fallback to user-token verification when the account endpoint is unavailable.

  Record first-page R2 access refusals (403, error 10003) for non-default jurisdictions in the required `FleetResourceInventory.unavailableR2Jurisdictions` array, preserving failures for default, later pages, and other errors.

  Export `FleetInventoryR2Jurisdiction` from the root and Cloudflare control-plane entries, and expose the root's reachable `D1FleetInventoryRunStoreOptions`, `D1FleetOperationStoreOptions`, `FleetInventoryDeploymentFactKind`, `FleetInventoryFailureReason`, `FleetInventoryGeneration`, `FleetInventoryRowKind`, `FleetInventoryRunProgress`, `FleetInventoryRunRecord`, `FleetInventoryStage`, `FleetInventoryStagedFact`, `FleetInventoryStagedRow`, `FleetInventoryStageInput`, `FleetInventoryStageResult`, `OrdinaryWorkerDeploymentVersion`, `PreparedOrdinaryWorkerDeploymentVersions`, and `PreparedOrdinaryWorkerUpload` types.

  Wait within the maintenance readiness deadline when a plain-Worker maintenance response attests the previous deployment specification, including version-override requests.

  Retry transient ordinary Worker provisioning failures up to three total backend attempts when provider reconciliation confirms that the upload, database or bucket creation, or deployment change did not take effect.

- 4f4da55: Encode Worker upload metadata as JSON, name module parts by their declared paths, and let fetch set the multipart boundary for ordinary, control, dispatch and state Worker uploads.
- 25c976a: Call supplied fetch functions without a client or backend receiver so native Worker fetch works in the Cloudflare client and maintenance backends.
- 6b55190: Allow ordinary Worker migration to inspect a previous release with different application variables, service bindings or queue bindings. Versions claiming the requested specification digest retain the binding checks.
- 2cdeed4: Use manual redirect handling for D1 export downloads so Worker fetch accepts the request and redirects remain rejected before export data is stored.
- f10379b: Forward configured `DeploymentSpec.subrequestLimit` values through ordinary-Worker uploads in the direct Cloudflare and Wrangler adapters, including staged versions. The setting was validated and included in the specification digest but omitted from upload requests. It now accompanies `cpuLimitMs`; an omitted setting remains unspecified.

  Review existing ordinary-Worker subrequest settings when upgrading because those configured values now reach the provider.

- 9111eb9: Validate plain Worker upgrades against the target application's bindings during candidate inspection, promotion, and settlement. Changing application variables or secrets no longer deploys the new bindings and then rejects them against the previous deployment's binding configuration.
- 0a09088: Reject incomplete Worker subdomain observations before interpreting ingress as disabled. Require explicit disabled flags for a present Worker through the ordinary and backend-switch proof paths while preserving an authoritatively absent parent.
- 121dd63: Validate inventory page structure and resource identity before recording absence, removing ingress or verifying secret revocation. Keep SDK pagination and the direct/Wrangler lookup paths consistent when metadata is incomplete.
- 6543c6f: `FileSystemDatabaseExportStore.write` no longer awaits the reader's cancel in its failure cleanup. When the body is a `tee()` branch, that cancel settles when the tee source is exhausted or errors, or the other branch is cancelled, so a write refused by the store's own checks held its rejection and its temporary file until then, and held both indefinitely when nothing drove the source. It now rejects with the store's error and removes the file without awaiting the cancel.
- 8c43533: Support signed maintenance for platform-authored Workers for Platforms catalogs. Catalog signing profiles can supply the maintenance keys without external-state artifacts. Catalog uploads receive their public verifier and local identity; FlowSafe relays capabilities while retaining the local receipt secret and validates the catalog script and digest before maintenance work.

  Existing catalog artifacts need a rebuilt FlowSafe runtime and explicit maintenance enrollment. The host configures the matching global dispatcher verifier.

  Persist catalog ownership explicitly in Fleet records and preserve it through native D1 migration and export-backed teardown. Catalog cleanup checks its own script and namespace authority. Force re-entry on completed or reserved records uses claim-releasing deletion when the store supports it.

  Preserve the prior mutable Worker schema identity while D1 advances and retain migration authority through compatibility teardown retries. Permit declared catalog binding changes with exact owner and uploaded-target checks.

  Allow ordinary spec-free force recovery after a candidate upload by clearing migration-only scalar fields when teardown begins, while preserving the recorded resource identity.

- 77e90e7: Use the platform URL hostname parser for inventory finding validation so the ordinary Worker control plane does not require node:url. Preserve international hostname validation, rejected-input behavior and original diagnostic text.

  Use crypto.randomBytes results directly for R2 reservation nonces and deployment-secret encoding, preserving their byte lengths and base64url representation.

- dd282de: Close staged Wrangler export streams before removing scratch files when a supplied store finishes or fails.
- Updated dependencies [c8c5039]
- Updated dependencies [4cb59a1]
- Updated dependencies [a027f13]
- Updated dependencies [a086f24]
- Updated dependencies [647092e]
- Updated dependencies [e79b92a]
- Updated dependencies [6bd8bfc]
- Updated dependencies [323c2ce]
- Updated dependencies [8c43533]
  - @proofoftech/flowsafe@0.21.0

## 0.4.0

### Minor Changes

- 1212ba5: Fleet Control now attests the release that is actually serving traffic after every package-owned promotion and supports lease-held, idempotent settlement callbacks.

  This changes the public control-plane contract:

  - **BREAKING:** `ProvisioningBackend.attestActiveRoute()` is required. Public `PlainWorkerRouteApi` implementations must also provide `inspectActiveWorkerRoute()`.
  - **BREAKING:** the fourth argument to `ProvisioningBackend.seedDeploymentIdentity()` is now `SeedDeploymentIdentityOptions`, shaped as `{ initialExecutionFenceState }`. Provisioning exports now include `InitialExecutionFenceState`.
  - **BREAKING:** `provisionDeployment()` requires `initialExecutionFenceState`, is asynchronous even when entry validation fails, and accepts `routeAttestation?`. Its ready record uses the attested routed `artifactVersion`, which can differ from the inspected candidate value recorded by earlier releases.
  - **BEHAVIOR CHANGE:** `provisionDeployment()`, every `migrateFleet()` promotion branch, and `rollbackExternalRelease()` attest after promotion and fail closed when routing is absent, split across two versions, or does not match the expected release. Earlier releases could complete from desired-state inspection alone.
  - Active-route exports now include `attestFleetRecordActiveRoute`, `attestConvergedActiveRoute`, `ActiveRouteAttestation`, `ActiveRouteAttestationError`, `ActiveRouteExpectation`, `AttestConvergedActiveRouteOptions`, and `ObservedActiveRoute`.
  - Settlement exports now include `fleetSettlementKey`, `FleetSettlementContext`, `FleetSettlementEntry`, and `FleetSettlementHost`. `migrateFleet()` accepts `settlementFor?` and `routeAttestation?`; `rollbackExternalRelease()` accepts `settlement?` and `routeAttestation?`.
  - Settlement callbacks run at least once under the deployment lease and are keyed by `settlementKey`. Hosts must deduplicate external effects by that key. The package records `FleetRecord.settledSettlementKey` so routine convergence skips an already recorded settlement while still attesting the route. Existing fleet databases add the nullable column automatically on first open.
  - Both backend constructors accept `clock?` for attestation timestamps.
  - The runtime dependency on `@proofoftech/flowsafe` is published as the exact matching release. A host pinned to an older Flowsafe must upgrade or its package manager can install a second copy, which the deployment boundary does not support.

  Implement the new backend methods before upgrading. Treat route ambiguity as a refusal, cache host-facing attestations instead of reading them per request, pass an explicit initial execution-fence state to provisioning and seeding, and make every settlement effect idempotent on `settlementKey`.

### Patch Changes

- Updated dependencies [1212ba5]
  - @proofoftech/flowsafe@0.20.0

## 0.3.4

### Patch Changes

- 66c19f1: Clean generated output at the packaging boundary so deleted source modules cannot remain in published tarballs.
- Updated dependencies [80a801c]
- Updated dependencies [fa0d11d]
- Updated dependencies [0447466]
- Updated dependencies [da6a0aa]
- Updated dependencies [8f4daae]
- Updated dependencies [66c19f1]
- Updated dependencies [5cbe01d]
  - @proofoftech/flowsafe@0.19.0

## 0.3.3

### Patch Changes

- Updated dependencies [e7fb658]
  - @proofoftech/flowsafe@0.18.0

## 0.3.2

### Patch Changes

- 8028605: Delete an already-decommissioned fleet ledger row without emitting a duplicate decommission event mislabeled as forced.
- Updated dependencies [37175fa]
  - @proofoftech/flowsafe@0.17.0

## 0.3.1

### Patch Changes

- Updated dependencies [296207f]
  - @proofoftech/flowsafe@0.16.1

## 0.3.0

### Minor Changes

- 1f6a13a: Add a fenced, replay-safe `forceDecommissionDeployment()` escape hatch for ordinary deployments whose retained specification inputs are unavailable.

  The operation always runs under `withDeploymentLease()` and persists the normal teardown phases. It removes control secrets, disables and verifies public ingress, detaches the custom domain, and deletes the exact persisted D1 database ID after asserting its fleet-owned name. Provider `404` responses converge as already absent, so retries resume after any completed mutation. Success deletes the fleet ledger row and emits the normal audit surface with `forced: true`.

  Force decommission never fetches an artifact, rebuilds a specification digest, or enters the `FLEET_SPEC_DIGEST` or version-ownership attestation path. It does not delete the ordinary Worker script, application R2 buckets, or control-plane retention data. After success, the host remains responsible for deleting its separate retention row and revoking its gateway key.

  The built-in `WranglerLoopBackend` implements the required spec-free route-API operations. Custom ordinary-Worker backends must implement `forceDecommissionStep()` with equivalent fenced checks. Workers for Platforms deployments fail closed because their dispatch and trusted-resource topology cannot use the ordinary-Worker route API. A `database-create-authorized` record also fails closed because its durable row cannot prove the exact database ID after a lost create response.

### Patch Changes

- Updated dependencies [1f6a13a]
  - @proofoftech/flowsafe@0.16.0

## 0.2.2

### Patch Changes

- 3df645d: Delete plain-worker D1 databases by immutable ID through Cloudflare's REST API instead of passing the UUID to `wrangler d1 delete`. Teardown now accepts provider 404 as an already-absent database, confirms exact-ID absence, and fails closed when a custom `PlainWorkerRouteApi` does not provide the required `getDatabase` and `deleteDatabase` capabilities.

## 0.2.1

### Patch Changes

- bec0029: Allow plain-Worker decommission to finish when Cloudflare secret deletion creates new Worker versions.

  Fleet Control now treats exact persisted artifact membership as the pre-mutation teardown gate during traffic removal. Later teardown steps accept provider-created version IDs, resolve the persisted artifact directly even after it leaves Wrangler's ten-entry list output, and validate every deployed version's tenant, environment, database, specification, schema, and ingress identity. Worker deletion retains resource-footprint checks and verifies full absence.

## 0.2.0

### Minor Changes

- 34e8ae0: Require host-provided Wrangler `>=4.118 <5` without installing it as a Flowsafe peer. Hosts that use `flowsafe-provision` must now install a compatible Wrangler version directly.

  Fleet Control now supports an explicit Wrangler command, creates D1 databases through Wrangler 4's current output contract, and revokes plain-Worker credentials through the Workers API without creating untracked Worker versions.

  Custom `PlainWorkerRouteApi` implementations must add `deleteControlSecrets(scriptName, secretNames, fence)`. Implement it with the Workers script-secret DELETE API, treat an HTTP 404 as already deleted, and retain a final authoritative secret-list check.

### Patch Changes

- Updated dependencies [2c097d8]
- Updated dependencies [34e8ae0]
  - @proofoftech/flowsafe@0.15.0

## 0.1.0

### Minor Changes

- fa12c05: Publish fleet control to npm as `@proofoftech/fleet-control`. Version 0.1.0 is the first release on the registry; 0.0.1 through 0.0.4 were repository-internal and were never published under the previous unscoped `anchorage-fleet-control` name.

  Fleet control remains control-plane software. It holds account credentials, routing ownership, billing policy, and tenant lifecycle, so a data-plane Worker must never import it. The registry no longer enforces that boundary, so enforce it in your build: confine the dependency declaration and every import to one provisioning service, and match subpath specifiers as well as the bare package name, because the three `./workers/*` entry points are importable on their own. Inside this repository the `fleet-control-is-control-plane-only` architecture rule fails the build when any other package under `packages/` reaches it.

### Patch Changes

- Updated dependencies [fa12c05]
  - @proofoftech/flowsafe@0.14.0

## 0.0.4

### Patch Changes

- Updated dependencies [352b38c]
  - @proofoftech/flowsafe@0.13.1

## 0.0.3

### Patch Changes

- Made provider-binding attestation consume every binding entry across plain, dispatch, backend-switch, and control Workers, and made durable Cloudflare API quota coordination use the Workers D1 binding interface. Production hosts must pass a direct binding so coordination traffic cannot consume the quota it protects; runtime structural validation cannot prove adapter provenance.

- Fixed expected-empty plain-worker binding attestation, replaced Wrangler SQL interpolation with fenced provider-native D1 parameters and batches, made fleet schema initialization retryable and concurrent-upgrade safe, and added durable cross-replica Cloudflare API quota coordination through an explicit nonsecret scope.

- Updated dependencies [4f0fc9d]
  - @proofoftech/flowsafe@0.13.0

## 0.0.2

### Patch Changes

- Updated dependencies [3276c2a]
- Updated dependencies [b3b4b55]
  - @proofoftech/flowsafe@0.12.0
