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

- d9f864f: Add a bounded, resumable fleet drift audit API with a durable, provider-neutral operation store. `advanceFleetAudit()` performs at most one bounded stage chunk per call — one global-stage slice of up to `maxItemsPerCall` items (1..2,000, default 500), or exactly one Fleet record's inspection and re-arm — against a `FleetOperationStore`; `D1FleetOperationStore` implements that port over the existing Fleet D1 binding with account-and-kind-scoped leases, lease-fenced guarded batches, and audit generation pinning. Call `start` with an operation id, the audited records, and `staleAfterMs`, then re-enqueue only the pending token each call returns. Start requires at most 10,000 records whose canonical bytes total at most 16 MiB, with each record within the 96 KiB staged-row byte bound and the per-record structure bounds: plain JSON data (no `undefined`-valued properties, dates, class instances, or cycles) within depth 64, 8,192 nodes, and 4 KiB per string value or object key. Every record must satisfy the deployment identifier grammar, and an explicit generation must be a positive safe integer. Every such refusal has a fixed message and precedes every durable effect. The `staleAfterMs` and operation-id refusals also occur before the lease; after the lease row is written, the foreign-kind, no-finalized-generation, and `auditClock`-sample refusals write nothing else. These accepted inputs are intentionally narrower than `auditFleetDrift()`, which does not require that identifier grammar, an explicit generation, or the bounded path's row and structure bounds. Read the findings back page by page with `readFleetAuditFindingsPage()` once the operation is terminal; `abandonFleetAuditOperation()` unblocks a stuck running operation and releases any pin an already-terminal one still holds.

  - `auditFleetDrift()` keeps its exact signature, refusal message, finding vocabulary, finding order, provider interaction order, return value, and stop behavior. It now drains the same decomposed stages in memory, and a frozen golden baseline (findings and the full store/backend/resolver call log) pins all of that.
  - **HARDENING:** the bounded engine never persists the raw diagnostic bytes a one-shot audit composes call-locally. The three resolvers, the inspection, the re-arm, and the segmented multi-duty `maintenance-stale` composition durably record a fixed template alone; any finding detail, composed by the engine or passed through from the pinned inventory generation, that fails a non-throwing credential-substring and control-byte gate persists a fixed withheld-detail fallback instead of aborting the operation.
  - An audit start pins exactly one finalized `@proofoftech/fleet-control` R3 inventory generation and keeps it through completion, so a finding page stays interpretable against the exact generation it was computed from; only explicit result garbage collection, terminal failure, or abandonment releases it. A replayed start never re-resolves "latest": it reuses the persisted generation.
  - Every finding or fact must fit the staged-row envelope: a 16 KiB JSON-serialized payload, 4 KiB per string, and the codec's depth and node bounds. The coordinator detects an excess before the store sees the row, fails the whole operation with the durable `emission-bound-exceeded` reason, and releases the pin. One record's whole per-call emission set (its findings plus the cross-record ownership facts it newly claims) must also fit inside the one guarded D1 batch its `per-record` call commits — a ceiling of 99 rows. A record whose live inspection alone would approach that ceiling fails with the same reason rather than emitting a partial finding set.
  - The bounded path differs from the drain in exactly four classes: every resolver, inspection, and re-arm failure and every multi-duty `maintenance-stale` finding persists one of the six fixed detail-template families where the drain composes the raw diagnostic; any unsafe finding detail, composed or passed through, becomes the fixed withheld-detail fallback; concurrent mutation can cause either a re-arm refusal on a Fleet reread mismatch or inspection-derived findings against later provider truth, with the bounded path's typically older snapshot making both more likely; and `emission-bound-exceeded` (from the staged-row envelope or the 99-row ceiling) and `generation-unavailable` are terminal whole-operation failures with no drain counterpart, while `auditFleetDrift()` completes and returns its full finding array over the identical world and clocks. Every other output is proven byte-for-byte equivalent to the drain under identical frozen worlds and clocks.
  - Per-call cost is not free: every advance call that runs a stage chunk re-reads the pinned generation in full and re-pages the accumulated `record` rows; a record-processing `per-record` call additionally re-pages the accumulated `fact` rows. Each stage-running call also structurally re-parses every accumulated `record` row through three plain-data traversals. `finding` rows are never re-paged. A stale token, a `start`, and a `finalize` call read neither. The complete aggregate cost of one bounded audit spans `1 + records + Σ max(1, ⌈source_i / maxItemsPerCall⌉)` stage-running calls: one per-record-to-finalize transition, one processing call per record, and at least one call per global source. Every such call re-reads O(G) generation rows and re-pages and structurally re-parses O(R) accumulated `record` rows; a record-processing `per-record` call additionally re-pages accumulated `fact` rows. In the records-dominated case, this is O(records) full generation re-reads, O(records²/1,000) accumulated-row page reads, O(records²) billed rows read, and O(records²) structural `FleetRecord` re-parses at three plain-data traversals each, the dominant CPU term. This checkpoint's in-memory suite measured about 0.25 ms per record-row re-parse per call: one per-record call over 1,001 accumulated rows took roughly 0.5 s in fix 7, and a 1,200-record full drain took roughly 122 s in fix 6. A late per-record call at the 10,000-record ceiling therefore spends seconds of isolate CPU re-parsing before its provider work. The per-call guarantee covers bounded provider work and bounded emission, not bounded CPU or bounded rows read. See the fleet control guide for the full envelope.

  No existing public export changes shape, and the Worker subpath is unchanged.

- 2600c07: Add a bounded, resumable account inventory API with durable generations. `advanceFleetInventory()` performs at most one provider stage chunk per call against a `FleetInventoryRunStore`; `D1FleetInventoryRunStore` implements that port over the existing Fleet D1 binding with operation-keyed runs, lease-fenced guarded batches, generation pinning, and bounded garbage collection. Build the provider seam with `cloudflareFleetInventoryContext(client)`, call `start` with an operation id, then re-enqueue only the pending token each call returns. The final call returns a `FleetInventoryGenerationRef`; read the rows back as today's `FleetResourceInventory` with `readFleetInventoryGeneration()`. Budgets are caller-supplied and validated: `maxProviderRequests` 9..1,000 and `maxStagedRowsPerChunk` 1..2,000 (default 500).

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

### Patch Changes

- f100ab3: Harden account-wide D1 and R2 attachment scans with a request-bounded, page-independent resumable engine. Rechecked inventory drift, malformed provider metadata, non-string or repeated dispatch cursors, and page or item overflows now fail closed instead of allowing an incomplete absence proof.
- 6543c6f: `FileSystemDatabaseExportStore.write` no longer awaits the reader's cancel in its failure cleanup. When the body is a `tee()` branch, that cancel settles when the tee source is exhausted or errors, or the other branch is cancelled, so a write refused by the store's own checks held its rejection and its temporary file until then, and held both indefinitely when nothing drove the source. It now rejects with the store's error and removes the file without awaiting the cancel.
- Updated dependencies [a086f24]
  - @proofoftech/flowsafe@0.20.1

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
