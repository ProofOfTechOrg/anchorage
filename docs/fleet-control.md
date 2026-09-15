<!-- Content type: Reference -->

# Provision physically isolated deployments

Fleet control provisions isolated application Workers and their D1/R2 resources for each project environment. Platform-authored deployments can use ordinary Workers or a Workers for Platforms catalog. External deployments keep trusted state and customer code in separate scripts within the untrusted dispatch namespace. Use the `@proofoftech/fleet-control` package from a trusted control plane, never from tenant request scope. The package is published, so enforce that boundary in your own build: see [Import it only from a trusted control plane](../packages/fleet-control/README.md#import-it-only-from-a-trusted-control-plane).

## Choose a provisioning backend

All backends implement the same ordered `ProvisioningBackend` contract:

| Backend | Accepted artifacts | Deployment mechanism |
| --- | --- | --- |
| `CloudflareApiPlainWorkerBackend` | Platform-authored only | Direct Cloudflare APIs for ordinary Workers and Worker Versions |
| `WranglerLoopBackend` | Platform-authored only | Host-provided Wrangler `>=4.118 <5` commands and generated configuration |
| `WorkersForPlatformsBackend` | Platform-authored catalogs and external project releases | Cloudflare Upload API in an untrusted dispatch namespace |

`WorkersForPlatformsBackend` requires its untrusted dispatch namespace, named shared outbound Worker, and state-egress root secret at construction. It rejects an incomplete configuration before provider access. External provisioning places its trusted state script in that dispatch namespace. Ordinary state scripts exist only as already-persisted bridges managed by the dedicated backend-switch lifecycle.

`WranglerCommandRunner` defaults to `['pnpm', 'exec', 'wrangler']`. Pass its `wranglerCommand` option when the host must select an explicit Wrangler executable or wrapper and fixed arguments. Fleet Control does not install Wrangler for the host.

`CloudflareApiPlainWorkerBackend` uses a `CloudflareProvisioningClient` constructed with `plane: 'plain-worker'`. Do not pass a dispatch namespace. Supply a durable `exportStore` to the client before any lifecycle can delete D1.

The plain backend rejects external artifacts before creating a resource. Switch to Workers for Platforms before you run the first customer-authored artifact. Set `routeHostname` to the customer-facing custom domain. For plain Workers, set `maintenanceBaseUrl` to the distinct Workers control origin; for Workers for Platforms, set it to the control-plane dispatcher origin. Fleet state reserves the route before any Worker can publish it.

## Run the trusted control plane in a Worker

Import `createCloudflareControlPlane` from `@proofoftech/fleet-control/cloudflare-control-plane` in a dedicated trusted Worker. The factory constructs the ordinary backend and durable stores from your bindings. Your host authorizes requested operations and resolves their specifications and credentials. Keep this Worker separate from tenant application Workers.

### Bind durable state and provider credentials

Use native D1 bindings for Fleet state and shared provider-quota reservations, plus a private R2 bucket for database exports. Replicas sharing a provider quota must use the same quota database and nonsecret scope. Preserve export bucket identity and receipt authority while operations remain active.

These example environment names belong to the host:

| Host value | Factory input |
| --- | --- |
| `FLEET_DB` D1 binding | `fleetDatabase` |
| `QUOTA_DB` D1 binding | `quotaDatabase` |
| `EXPORTS` R2 binding | `databaseExports.bucket` |
| `CLOUDFLARE_ACCOUNT_ID` variable | `accountId` |
| `CLOUDFLARE_API_TOKEN` secret | `apiToken` |
| `CLOUDFLARE_QUOTA_SCOPE` variable | `quotaScope` |
| `EXPORT_BUCKET_NAME` variable | `databaseExports.bucketName` |

Set the nonsecret variables in your Worker configuration. Configure the provider token through [Workers secrets](https://developers.cloudflare.com/workers/configuration/secrets/), using the permissions in the [direct backend threat model](security-threat-model.md#direct-cloudflare-api-backend). Never put the token in checked-in variables, Queue messages, or tenant bindings. Keep the export bucket private through its R2 access configuration.

This configuration fragment binds a Queue consumer and its continuation producer. Replace the resource identifiers with your dedicated control-plane resources:

```jsonc
{
  "name": "fleet-control",
  "main": "src/index.ts",
  "compatibility_date": "2026-08-06",
  "workers_dev": false,
  "preview_urls": false,
  "d1_databases": [
    { "binding": "FLEET_DB", "database_id": "your_fleet_database_id" },
    { "binding": "QUOTA_DB", "database_id": "your_quota_database_id" }
  ],
  "r2_buckets": [
    { "binding": "EXPORTS", "bucket_name": "fleet-exports" }
  ],
  "queues": {
    "producers": [{ "binding": "CONTROL_QUEUE", "queue": "fleet-jobs" }],
    "consumers": [{ "queue": "fleet-jobs", "max_batch_size": 1 }]
  }
}
```

`max_batch_size: 1` is an example scheduling choice. Set retry, concurrency, and dead-letter handling for your workload; see the [Wrangler configuration reference](https://developers.cloudflare.com/workers/wrangler/configuration/). Disabling workers.dev and preview URLs does not authenticate a management endpoint. If you expose one, authenticate and authorize its callers before invoking Fleet.

The required type peer is `@cloudflare/workers-types >=5.20260730.1 <6`; this repository verifies `5.20260905.1`. Import the factory at module scope:

```typescript
import { createCloudflareControlPlane } from
  '@proofoftech/fleet-control/cloudflare-control-plane';
```

Inside the event handler, construct it from the host's typed `env`:

```typescript
const control = createCloudflareControlPlane({
  accountId: env.CLOUDFLARE_ACCOUNT_ID,
  apiToken: env.CLOUDFLARE_API_TOKEN,
  fleetDatabase: env.FLEET_DB,
  quotaDatabase: env.QUOTA_DB,
  quotaScope: env.CLOUDFLARE_QUOTA_SCOPE,
  databaseExports: {
    bucket: env.EXPORTS,
    bucketName: env.EXPORT_BUCKET_NAME,
    streams: { DigestStream: crypto.DigestStream, FixedLengthStream },
    randomUUID: () => crypto.randomUUID(),
  },
});
```

The Queue binding and any management authentication secret belong to your handler. They are not factory options. Continuation tokens identify work to compare against Fleet D1; the factory has no token-signing secret.

### Advance work before acknowledging delivery

Cloudflare Queues can deliver messages more than once and out of order. Keep specifications, secrets, account selection, and provider configuration in trusted host state. Queue payloads carry operation claims and returned continuation tokens. See [delivery guarantees](https://developers.cloudflare.com/queues/reference/delivery-guarantees/) and [Queue ordering](https://developers.cloudflare.com/queues/reference/how-queues-works/).

For a consumer-owned continuation handler:

1. Authorize the job and resolve its trusted inputs. Invoke the appropriate factory advance using the existing action and token types.
2. For `pending`, await sending the returned continuation claim before acknowledging the input message. If sending fails, retry the input. A crash after send and before acknowledgement can produce duplicate claims.
3. For `complete`, handle the durable result before acknowledging. Retain receipt-backed results according to their lifecycle.
4. For `blocked`, record the cleanup or decommission result for remediation and retain its returned token before acknowledging. After remediation, submit an authorized `restart-blocked` action with the current token. Do not automatically continue or restart blocked work.
5. For terminal `failed`, report the durable failure through your job-status or failure policy before acknowledging or dead-lettering. A resolved call does not imply success. Do not endlessly enqueue the same failed token.
6. Retry thrown transient failures and lease contention under your consumer policy. An exception does not prove an earlier provider or database effect failed to commit. Do not synthesize a newer token.

Use the per-message [`ack()` and `retry()` APIs](https://developers.cloudflare.com/queues/configuration/javascript-apis/). Unacknowledged batch failures can redeliver other messages; see [batching and retries](https://developers.cloudflare.com/queues/configuration/batching-retries/). Sending a continuation and committing Fleet progress are separate operations. Queue acknowledgement does not replace the database commit or make provider effects exactly once.

### Move a future executor between Node and Workers

An executor cutover preserves the deployment resources and durable authority it already manages. It differs from the [tenant physical-isolation cutover](deployment-reference.md#cloudflare-compatibility). Check state and receipt compatibility before moving future operations:

1. Verify that the destination version can read the existing Fleet schema, records, tokens, and receipts. Preserve the account, Fleet database, immutable resource mappings, trusted specifications, and credential source.
2. Preserve the shared quota database/scope and active export receipt authority. Finish filesystem-backed export operations under their original authority before moving them to a Worker that cannot supply it.
3. Stop new work submission to the source executor and let active calls finish or reach durable outcomes. Confirm lease ownership; a guessed lease timeout is not a drain. Stop the old scheduler and other authorized writers too.
4. Deploy and verify the destination without tenant traffic routes, then enable its job intake. Resume supported claims against Fleet D1. Do not import process-local cursors or rewrite operation state from memory.
5. For rollback, stop Worker intake first. Resume Node execution only with a version that understands the states and receipts already written. Otherwise roll forward with a compatible executor. Never restore an old Fleet snapshot over newer provider effects.

You can [pause and resume Queue delivery](https://developers.cloudflare.com/queues/configuration/pause-purge/) during the transition. Pausing still allows messages to arrive and expire; it does not terminate an invocation already running.

### Size the Worker for its workload

The supported packed configuration is `2026-08-06` without explicit compatibility flags. Cloudflare enables Node compatibility by date from [2026-08-04](https://developers.cloudflare.com/changelog/post/2026-08-04-nodejs-compat-default/). The historical comparison uses `2026-08-03` with `nodejs_compat,no_nodejs_compat_v2`, and with `nodejs_compat`.

Use Workers Paid for the direct control plane. The operation-specific request bounds below remain distinct from Cloudflare's platform allowance. Check the current [Workers limits](https://developers.cloudflare.com/workers/platform/limits/) and [D1 limits](https://developers.cloudflare.com/d1/platform/limits/) before sizing the host. As checked on 2026-09-09, the Worker bundle limit is 64 MiB uncompressed, memory is 128 MB per isolate, and startup has a one-second limit. Gzip has no platform size limit. Concurrent invocations share isolate memory.

Inventory reads and audit continuations materialize stored data. Follow the [audit memory/read-cost envelope](#audit-an-account-under-a-request-budget) and [migration recovery and cost boundaries](#recovery-and-cost-boundaries). Provider request budgets and input validation ceilings do not guarantee that a workload fits memory, CPU, or database query limits.

At commit `308bd39`, the unminified namespace-import fixture measured 3,292,290 raw bytes and 448,465 gzip bytes in the supported and historical configurations, with zero size delta. Its repository regression budgets are 4,128,768 raw bytes and 589,824 gzip bytes: 25% headroom rounded upward to 64 KiB. Reproduce the package checks with `pnpm test:packed-fleet-control`.

The supported configuration's local startup profile sampled 21.690 ms of active CPU within a 102.108 ms profile window. The workload fixtures completed a 32-record audit, selected late continuations for 1,001 and 10,000 records, and a selected continuation after exact 16 MiB intake. Larger cases seed prior observations and cursors; they do not execute the preceding provider calls. These are dated local observations, not production latency or capacity guarantees.

The harness does not expose CPU or peak isolate-memory counters. [Local Wrangler does not enforce production runtime limits](https://developers.cloudflare.com/workers/wrangler/configuration/#limits). Its 1,001-record intake made 46 Fleet-binding calls containing 1,038 SQL statements; the 10,000-record intake made 135 calls containing 10,037 statements. Binding calls and SQL statements are different counters. The published D1 query limit and Workers subrequest allowance do not establish how those batch contents are counted here; production query-budget compliance remains unverified.

## Provision a deployment

Use the [factory above](#run-the-trusted-control-plane-in-a-worker) in a Worker. For an explicitly composed host, create a backend, durable `FleetStateStore`, validated `DeploymentSpec`, and distinct credentials. `provisionDeployment()` applies this order:

1. Create or resolve the uniquely named D1 database
2. Seed and verify the shared deployment-identity sentinel
3. Apply hash-verified D1 migrations in version order
4. Reserve, create, and persist every fleet-owned application R2 bucket before any application Worker receives its binding
5. For an external deployment, provision its stable platform-authored state script in the untrusted dispatch namespace, then persist its exact ownership and artifact snapshot
6. Upload the application Worker with its canonical variables, secrets, R2 bindings, D1 binding, and Durable Object bindings
7. Address the exact uploaded version through the authenticated control path, require its specification-digest attestation, and call `POST /admin/ensure-maintenance`
8. Persist publication intent, publish the host mapping, verify the live resource graph, and persist `ready`

```typescript
const result = await provisionDeployment({
  backend,
  store,
  spec: deployment,
  secrets: generateDeploymentSecrets(),
  initialExecutionFenceState: 'open',
});

if (!result.maintenance.armed) {
  throw new Error('maintenance did not arm');
}
```

The function persists every completed phase and validates the immutable tenant, environment, logical script, database, and route mapping on retry. Database creation advances from `database-reserved` to an explicit create-authorized ownership phase before the provider mutation, so a same-name discovery cannot be silently adopted after a crash or race. `D1FleetStateStore` serializes each deployment lifecycle with a renewable database-time lease and fences state writes with its owner token. It commits the exact Worker, dispatch-script, and R2 claims with the fleet row in one D1 batch. The final statement fails the whole batch if the lease expired after an earlier claim statement. A lost batch response converges only after raw serialized row values, the complete owned claim set, desired-key occupants, and the live lease all match. Specification digests make a retry use the exact intended modules, bindings, limits, and migrations. A changed ready specification must go through `migrateFleet()`.

Provisioning resumes from its last durable phase without repeating a committed step. Before `ready`, it compares the exact live tenant, environment, D1 binding, schema, specification digest, Durable Object bindings, plain-text variables, and secret names. Plain Worker, dispatch Worker, backend-switch, and control-plane inspection must consume every raw provider binding entry. An unknown type, malformed entry, duplicate name, binding absent from the structured inspection, or missing complete inventory fails closed even when the desired application groups are empty. Ordinary Worker secret names come from the authoritative secret-list API; if version resources also report them, the two inventories must agree. A failed first create rolls back through the bounded cleanup engine when the deployment provably never authorized a candidate invocation: rollback persists a durable `provisioning-rollback` cleanup intent, revokes credentials, removes the resources this attempt created, and completes with an immutable terminal receipt in place of a bare row delete. A rollback the engine refuses — a Workers for Platforms or external-artifact candidate, an already authorized invocation, or a stack without the bounded capabilities that keeps the in-memory rollback — preserves the row at its phase. If an upload may have succeeded, cleanup treats the Worker as present until deletion is positively confirmed; D1 is never deleted while a Worker or route may remain. Cleanup errors remain attached to `ProvisioningError.cleanupErrors`, and the durable state remains available to retry or to `cleanupDeploymentArtifacts()`; see "Clean up failed provisioning with durable receipts".

`provisionDeployment()` reads a retired terminal record as an absent prior. A stored `decommissioned` row qualifies when its `isRetiredTerminalRecord` predicate holds: the row carries the record a completed decommission leaves — its `applicationResources` entries `deleted`, its export location, digest, and byte count recorded, and no pending lifecycle field — and no unfinished decommission, cleanup, or backend-switch operation. The slug and the specification-derived names — logical script, database, and route hostname — come from the supplied `DeploymentSpec`, equal to the retired row's when that specification retains them. The database and its provider-minted ID, the seeded deployment identity, the application R2 resources, and the artifact version are new, and the replacement carries no export location, digest, or byte count. The terminal row is replaced under the deployment lease. A changed specification is accepted, because the immutable-mapping and digest guards read a prior and this record is not one; `previousDurableObjectTag` is refused as it is for any other new deployment. A terminal row that retains an application resource, or whose teardown evidence is incomplete — a force-produced row records no database export — refuses instead, names that reason, and directs to `forceDecommissionDeployment()` once the residual physical resources are confirmed removed; an unfinished decommission, cleanup, or backend-switch operation refuses through the guard that owns it, as it does from any other lifecycle entry. A host that wants to keep the database and the seeded identity migrates with `migrateFleet()` instead of decommissioning. An audit sweep whose record snapshot predates the re-provision reads the old terminal row beside the new Worker and can report orphan findings until its next tick.

`initialExecutionFenceState` is required and accepts `open` or `migration-locked`. It is a provisioning decision, not part of `DeploymentSpec`, so it does not alter the specification digest. `provisionDeployment()` is asynchronous from entry: invalid initial state and other validation failures reject its promise instead of throwing before a promise exists. The final ready record uses the artifact version from post-promotion route attestation rather than the candidate inspection.

## Define application bindings

`DeploymentSpec.application` declares three canonical binding groups:

- `vars`: plain-text names and values
- `secrets`: names and UTF-8 SHA-256 value descriptors
- `r2Buckets`: binding names and optional `default`, `eu`, or `fedramp` jurisdictions

Fleet control sorts each group by binding name before hashing, persistence, rendering, and comparison. Reordering a declaration does not change its specification digest. Variable values, secret names and digests, and R2 descriptors do change that digest. Plain-text variables are not confidential.

Binding names share one namespace with fleet variables, D1, Durable Objects, queues, services, and R2. Validation rejects duplicates, cross-category collisions, reserved fleet names, and the `FLEET_` or `DEPLOYMENT_` prefixes. Omitting `application` is equivalent to declaring three empty groups.

Pass application secret plaintext only through `DeploymentSecrets.application` at the trusted invocation seam. Fleet control requires its keys to equal the declared secret names and verifies every supplied value against `valueSha256` before any provider request. It never persists or logs those values. Cloudflare exposes secret names after upload but no comparable value digest, so recurring inventory can attest the exact name set but cannot detect a value-only change made outside fleet control. Reconcile from the trusted secret provider to rewrite the intended value.

Application R2 buckets belong to the deployment, not to a release. Fleet control derives each physical bucket name, permanently claims it for that deployment, records create authorization before calling Cloudflare, and persists the created mapping before application upload. A retry can adopt an exact same-name bucket only after that authorization records a potentially committed create. One per-bucket state machine drives initial provisioning rollback, manual cleanup, ordinary decommission, and backend-switch teardown: `reserved`, `create-authorized`, `created`, `detach-authorized`, `detached`, `empty-authorized`, `empty`, `delete-authorized`, then `deleted`. Provider absence is accepted only from a durable mutation-authorized state, and D1 cleanup cannot pass an unresolved bucket. R2 binding names, physical bucket names, and jurisdictions remain immutable across migration, rollback, and backend switch.

Application Workers receive the application bindings. A plain platform-authored Worker receives both built-in credentials and its declared application secrets. An external candidate receives its deployment-identity credential and declared application secrets, but never the maintenance credential. Trusted state, outbound, dispatcher, and audit Workers receive no application variable, application secret, or application R2 binding. During a backend switch, the legacy bridge keeps the prior plain release's application bindings while it serves application fetches. The target external candidate receives the target release's bindings, and the final state-only bridge receives none.

Application KV bindings are unsupported. Cloudflare caps an account at [1,000 KV namespaces](https://developers.cloudflare.com/kv/platform/limits/), below fleet control's 10,000 project-environment horizon. Sharing an application namespace would restore a logical tenant-isolation boundary. The platform-owned `HOSTS` namespace remains limited to control-plane hostname publication and is never an application binding.

### Stage ordinary Worker versions

Both ordinary-Worker backends upload a digest-tagged Worker Version with the exact built-in and declared application secret set. `WranglerLoopBackend` passes those secrets through a mode-0600 file. For an existing deployment, either backend attaches the candidate at zero percent, sends the maintenance request with `Cloudflare-Workers-Version-Overrides`, and accepts the response only when `deploymentSpecDigest` matches the requested build. It then promotes that version to 100 percent, verifies the live custom-domain owner against `PromotionGuard`, attaches the domain, and re-inspects the mapping. A failed maintenance check never publishes the route.

The direct API backend must create an initial script before Cloudflare accepts its workers.dev configuration. For either ordinary-Worker backend, tagged-version rediscovery accepts a failed upload only after the Worker footprint attests the intended workers.dev and preview-URL state. Worker uploads, D1 and R2 creation, and deployment changes retry transient provider failures up to three total backend attempts, with 2-second and 4-second delays, after readback proves the intended effect is absent.

Both built-in ordinary-Worker backends run the same conformance suite. It verifies fleet records, backend results, provider-visible Worker versions and bindings, deployment percentages, secret names, domains, databases, Durable Object namespaces, export bytes and integrity, initial-deploy public access, and mutation ordering where order affects safety. It deliberately excludes transport requests and commands, fence-call counts, pagination mechanics, export locations, provider diagnostics, and adapter scratch-cleanup outcomes. On staged uploads, the direct API adapter also converges workers.dev and preview-URL settings, while `wrangler versions upload` does not; the shared suite therefore does not assert staged public-access state.

When that workers.dev write keeps failing, the refusal's compensating traffic removal also fails and leaves the Worker in the account for operator cleanup; the provisioning error reports `resourceState: 'unknown'`.

The credentialed lane exercises the configured CPU limit at runtime through its `cpu-control` and `cpu-over-limit` probes.

Cloudflare error `10220` can refuse a deployment when a secret changed after its version upload. This provider limitation applies to both ordinary-Worker backends. Upload a new candidate carrying the current secret set before deploying it.

An initial Worker that introduces Durable Object classes uses a route-free `wrangler deploy`, validates maintenance through its distinct control origin, and publishes the customer domain afterward. An existing plain Worker cannot stage a new Durable Object lifecycle migration through Workers Versions; perform that change at an explicit immediate-deployment maintenance boundary. No generated configuration contains a cron trigger.

The Wrangler backend uses the direct Cloudflare D1 query and batch APIs for database ownership and application migrations, inside the same external mutation-fence boundary as its other provider writes. SQL and ordered scalar bindings remain separate until D1 parses them. Anonymous `?` and numbered `?NNN` parameters therefore coexist safely with literal question marks, escaped strings, quoted identifiers, and comments. D1 does not currently support named SQLite parameters. Each migration and its ledger row execute in one provider-native atomic batch.

Every generated plain Worker uses a platform-owned guarded entry module. The guard recognizes only the normalized `routeHostname` and `maintenanceBaseUrl` hosts. The route host rejects `Cloudflare-Workers-Version-Overrides` before it invokes application code. The control host accepts only `POST /admin/ensure-maintenance` and `GET /admin/maintenance-status`, and it requires the exact maintenance administrator bearer secret.

Every other hostname returns HTTP 404. This includes `workers.dev` unless `maintenanceBaseUrl` names that exact host. Preview URLs remain disabled.

A host Worker that runs an ordinary-Worker backend and fetches tenant maintenance origins on the same account's workers.dev subdomain must enable `global_fetch_strictly_public`. Otherwise, Cloudflare answers these same-subdomain Worker-to-Worker requests with error 1042, an HTTP 404 `text/plain` page. The direct conformance CLI requires this compatibility flag on its reference Worker.

After a deployment change, the maintenance route can answer a version-override request from the previous version for a few seconds (1–5 seconds in conductor measurements). When a valid maintenance health attests a different specification digest, the plain-Worker backend re-sends the request at `maintenanceRouteReadyIntervalMs` within `maintenanceRouteReadyTimeoutMs` before failing. This also bounds stale health answers during inspection without an override; platform 404 retries and digest retries share one deadline per maintenance request.

The guard re-exports local Durable Object classes and invokes the original default object’s `fetch` method with its original receiver. The main module must be importable string JavaScript whose evaluated default export is an object with a callable `fetch`; module evaluation fails closed otherwise. A versioned binding prevents a same-spec version created before this guard existed from satisfying candidate convergence.

## Promote and roll back external releases

Workers for Platforms user Workers have no public version-selection or gradual-deployment API. A same-name upload immediately replaces the live script at 100 percent. Fleet control therefore treats `DeploymentSpec.scriptName` as a logical project name and derives a distinct, content-addressed physical script name for every external specification.

`migrateFleet()` persists an exact target before it mutates D1 or any Worker. The target covers the candidate specification, prior and target releases, trusted state artifact and state-egress credential digests, Durable Object migration history, complete D1 migration history, schema version, outbound policy, audit queue, and maintenance verifier. The operation then records `schema-applied`, `platform-applied`, `candidate-deployed`, `candidate-armed`, and `route-published` subphases. A retry compares the requested target with that durable intent and resumes from the last completed boundary. A platform-only migration reuses the active release without creating a duplicate rollback snapshot. It never recomputes a changed profile into an in-progress migration.

Before any migration mutation, the pending release owns its canonical application variables, secret descriptors, and resolved R2 mapping independently of the deployment-wide active projection. After the D1 expand migration commits, `migrateFleet()` reconciles the trusted state script, uploads and validates the candidate against that pending topology without changing customer traffic, arms it through the maintenance-only dispatcher path, and publishes the host mapping. The prior physical script remains registered with its own application topology as the rollback release. `rollbackExternalRelease()` requires the exact retained specification digest, records rollback intent before changing the route, validates the retained script against the retained topology, and restores the deployment-wide application projection when it swaps the active and rollback release snapshots after publication. The fleet record keeps the applied D1 schema version monotonic and stores each release's supported schema separately, so rolling traffic back never claims that an applied migration was undone.

Host publication uses Workers KV because the dispatch Worker needs a low-latency hostname lookup. The same canonical host record supplies policy context and the SHA-256 state-egress credential digest to the one shared outbound Worker. The state script calls only its named `StateEgress` entrypoint. The shared outbound Worker verifies the exact tenant, environment, resource group, state script, route, policy, and credential before origin fetch. It strips the reserved context headers before sending the request. Every Workers for Platforms fleet record persists the canonical policy independently of a release. External deployments use the organization allowlist from their trusted profile, while platform-authored deployments default to a tenant-bound deny-all policy. Promotion and rollback publish that persisted policy, and drift recomputes and compares its identity, hosts, and digest. Workers KV is eventually consistent, so the durable fleet record and deployment lease remain the ownership authority. During propagation, requests may reach either retained compatible release. Do not delete the prior script until a later successful release retires it.

The state-egress credential digest is immutable for an existing trusted state resource. Rotating the root secret requires a coordinated credential migration that updates the state Worker secret and canonical host target as one durable lifecycle. The current provider can inspect the exact secret-name set, but Cloudflare does not expose secret values for attestation. Fleet control therefore rejects an implicit digest change before provider mutation.

External candidates cannot own Durable Object classes or migrations, and an external specification cannot choose a state script, dispatch namespace, outbound target, or physical R2 bucket name. The trusted control plane resolves every requested Durable Object binding to the deployment's stable state script and every R2 descriptor to the fleet-owned deployment resource. The state script owns FlowSafe runs, approvals, alarms, and Durable Object code while candidates remain independently replaceable. The state script receives the named `OUTBOUND_PROXY` service binding, its context-bound credential, the audit queue, and the maintenance secret. The candidate receives none of them. If audit export is enabled, its `AUDIT_PROXY` binding is a remote Durable Object binding to `FlowsafeFleetAuditProxy` in the exact state script. Fresh state scripts include the dispatch namespace in that binding; adopted ordinary bridge scripts omit it. Fleet state persists the exact state binding inventory and an append-only snapshot of every authoritative namespace ID. Drift and teardown use that snapshot rather than recomputing names. Every D1 migration introduced while a rollback release is retained must be explicitly marked rollback-compatible and use expand-only schema changes. Apply contract changes only after the rollback window closes.

Supply a trusted `platformProfileFor(spec)` provider for Workers for Platforms maintenance. Platform-authored specifications accept a `MaintenanceSigningProfile` containing the canonical `maintenanceCapabilityPublicKey` and matching `maintenanceCapabilityPrivateKey`. External specifications require the complete `ExternalPlatformProfile`, including the trusted state artifact, state migration history, organization egress allowlist, and optional legacy bridge template. Fleet control validates and hashes that profile before upload. A profile or policy change requires `migrateFleet()`, even when the customer candidate bytes are unchanged. Customer modules are never reused in fresh trusted state. The stable state name derives from immutable deployment identity, not candidate contents, so release promotion and rollback do not replace platform state.

### Configure signed catalog maintenance

Use the same maintenance public verifier in the global dispatcher and catalog signing profile. The host owns global-plane configuration and attestation through `provisionPlatformPlane`; the backend does not derive dispatcher ownership from the maintenance URL. Keep the private signer in the trusted host.

`WorkersForPlatformsBackend.deployWorker` enrolls catalog uploads with the public verifier, `FLEET_MAINTENANCE_CAPABILITIES=required`, `FLEET_RESOURCE_ROLE=platform-catalog`, and their physical script identity. The local Maintenance object retains its distinct `MAINTENANCE_ADMIN_SECRET` for receipt signing. Use a catalog built with the FlowSafe runtime supporting this mode. That runtime binds verified maintenance capabilities to its local script and release digest before reading health or consuming an ensure nonce. External stable-state maintenance continues to serve its retained candidate releases.

Catalog records persist `wfpMode: "platform-catalog"` from their first reservation. Custom Fleet stores must retain that field. The D1 store upgrades its schema additively; unmarked WFP records retain external-state ownership rules. Changing or dropping an established catalog mode is refused.

Importing an unmarked legacy catalog requires its original platform-authored specification. Under the existing deployment lease, verify its exact stored specification digest, immutable resource mapping and provider artifact ownership. Resolve active lifecycle operations and conflicting external resource authority before changing the mode. Check that any derived external-state reservation has no physical resource before releasing that claim. Write the marker through the store’s normal claim transaction; missing original authority cannot be replaced by the absence of state fields.

For runtime enrollment, rebuild the catalog with the corrected FlowSafe runtime. Under its deployment lease, call `backend.deployWorker` with the owned D1 reference, recorded application topology and original secrets. Set the target specification’s `previousDurableObjectTag` to the recorded tag. Retain the returned artifact without fabricating a ready Fleet record, then use `migrateFleet()` to reconcile the uploaded target. A terminal failed bounded migration operation remains terminal; start a separately authorized operation after resolving its failure.

Inspection requires enrollment and returns signed health for the provider-observed release. During unpinned target discovery, that can be the previous catalog digest. An explicit artifact expectation and maintenance ensure require the requested specification. Deployment refuses an enrolled catalog verifier change; coordinate verifier migration separately.

### Attest the active route

Active-route attestation proves which provider artifact receives traffic and which fleet specification digest that artifact carries. It starts from provider routing state, not the desired candidate returned by `inspect()`. Every package-owned promotion path attests after promotion: initial provision, every `migrateFleet()` branch, and `rollbackExternalRelease()` all fail closed when the route is absent, ambiguous, or mismatched. The two-version staging window is refused if observed, but the package's own promote paths never observe it because each attestation runs after promotion converges.

An ordinary Worker attestation performs two provider reads per attempt: read the deployment traffic split, require exactly one version at 100 percent, then read that version's specification-digest binding. A deployment containing two versions is refused even when one has 100 percent and the other has 0 percent; Fleet control never selects a version by highest share. `physicalScriptName` equals `spec.scriptName` by construction because promotion already proves custom-domain ownership for that script.

A Workers for Platforms attestation performs three provider reads per attempt: read the hostname mapping, resolve the routed physical script, then inspect that script and its digest. The shared convergence helper retries provider propagation for up to 60 seconds by default. Backend constructors accept `clock?` so `observedAt` uses the host's clock.

Use `attestFleetRecordActiveRoute()` for a single host-side drift read. Cache its result and never invoke it for every status request: one record costs two provider reads on the plain backend or three on Workers for Platforms. Use `attestConvergedActiveRoute()` only around a promote path that must wait for provider convergence.

### Settle a promotion under the lease

Pass `settlementFor?` to `migrateFleet()` or `settlement?` to `rollbackExternalRelease()` to run a host callback after the route attests and matches, but before Fleet control writes the settling state. Each callback receives `FleetSettlementContext` with a stable `settlementKey`. Delivery is at least once: a crash after `settle()` succeeds but before the state write repeats the callback under the same key. The host must deduplicate every external effect on `settlementKey`.

`alreadySettled` is an observational hint, not a deduplication guarantee. It can be false again after a successful callback whose following state write failed. `FleetRecord.settledSettlementKey` lets a later routine convergence skip a settlement whose key was durably recorded, while route attestation still runs.

The package imposes no callback timeout. By default, the state store renews the lease every five minutes against a 15-minute lifetime for as long as `settle()` runs. A hung callback that keeps renewing therefore holds the deployment lease indefinitely. Keep the callback brief and enqueue slow work. If the process dies, the lease expires 15 minutes after its last successful renewal by default. A throw leaves the migration re-enterable; the next call re-attests and retries the same settlement key.

The `entry` field distinguishes `migration`, `platform-only`, `ready-convergence`, and `rollback`. For the first three, `prior` is the replaced release when the backend still retains it. For `rollback`, `prior` is the abandoned active release. `prior` is optional-normal, and settlement logic may depend only on the target identity.

Initial provisioning consults no settlement host. It returns to the caller after route attestation, and the host can settle after return with `attestFleetRecordActiveRoute()`.

### Upgrade from 0.3

The 0.4 public surface has these breaking requirements:

- `ProvisioningBackend` implementations add required `attestActiveRoute()`
- Public `PlainWorkerRouteApi` implementations add required `inspectActiveWorkerRoute()`
- `ProvisioningBackend.seedDeploymentIdentity()` takes `{ initialExecutionFenceState }` as its fourth argument through `SeedDeploymentIdentityOptions`
- `provisionDeployment()` requires `initialExecutionFenceState`, returns its validation failures asynchronously, accepts `routeAttestation?`, and records the routed artifact version
- `migrateFleet()` accepts `settlementFor?` and `routeAttestation?`
- `rollbackExternalRelease()` accepts `settlement?` and `routeAttestation?`
- `WranglerLoopBackend` and `WorkersForPlatformsBackend` constructors accept `clock?`
- `FleetRecord` adds `settledSettlementKey`; `D1FleetStateStore` adds the nullable text column automatically on first open

Fleet control 0.4 depends on exactly one matching Flowsafe 0.20 runtime copy. Upgrade a direct Flowsafe 0.19 dependency at the same time to avoid a second nominal runtime copy.

## Upgrade a fleet in resumable steps

Use `advanceFleetMigration()` when a driver must release execution between migration steps. It uses the same deployment mutation engine as `migrateFleet()`, with a separate durable operation record and frozen per-item plan. This is the ongoing upgrade path for durable deployments, not an import mechanism for a previous provisioner's state. The root package remains a trusted control-plane entry; this API alone does not make the root entry Worker-compatible.

Construct `D1FleetOperationStore(database, { accountId })` over the Fleet database port, or supply a conforming `FleetOperationStore`. Keep the existing `FleetStateStore`, backend, specification and secret resolvers. The bounded options name the deployment store `fleetStore`; the one-call drain still names it `store`. Both paths accept the existing finalized-state, settlement, route-attestation and clock options.

Persist a caller-minted lowercase UUIDv4 operation ID and the original start input before dispatch. In this example, `migrationOptions` contains the trusted stores and resolvers, and `records` is the caller's fleet snapshot:

```typescript
import { advanceFleetMigration } from '@proofoftech/fleet-control';

const result = await advanceFleetMigration({
  ...migrationOptions,
  action: {
    kind: 'start',
    operationId,
    records,
    canaryTenantTags: ['canary'],
  },
});
```

A start snapshots its input before awaiting, preserves the drain's canary ordering, and stages only item keys, rank, provenance digest and pending status. Repeated canary tags use their last position; equal canary ranks remain stable. Start invokes no deployment resolver or provider. If staging stops at revision zero, replay the same start with its original input; a continue cannot reconstruct that input from a token.

For a pending result, durably enqueue its token. The next delivery makes one call using the same trusted options:

```typescript
const result = await advanceFleetMigration({
  ...migrationOptions,
  action: { kind: 'continue', token },
});
```

The first item call reads current Fleet state under its deployment lease and records the target specification digest, frozen plan and cursor zero without mutating the deployment. Later calls re-read Fleet state, re-resolve trusted inputs, validate the frozen target and plan, then execute one step. One step can include several provider requests; this API has no provider-request budget option. After the last item finishes, another call finalizes the operation. A complete result returns counts and finalization time, not a replacement fleet snapshot.

Stale tokens return current durable authority without provider work. Future, unknown-operation and wrong-kind tokens fail closed. A replayed start must have the same intake digest: a progressed operation returns current authority rather than replacing its items. Treat tokens as continuation claims, never as authorization to select an account, backend, specification or credential.

The limits apply together:

- At most 10,000 records and 16 MiB summed across canonical record bytes
- At most 96 KiB per input record; plain JSON within depth 64, 8,192 nodes, and 4 KiB per string or object key
- Deployment identifier grammar for every record's tenant and environment
- At most 64 frozen plan entries per item
- Item page and explicit prune limits from 1 through 1,000

The canary envelope is separately checked by the same plain-data and byte codec, then included in the intake digest. Its bytes do not count toward the record sum.

An admission or step error invokes one failure commit for the item and operation. After that commit succeeds, the original error is rethrown to the trusted caller; later ordinals do not run. Durable failure data contains only the reason and optional item ordinal, never the original exception or secrets. `target-drift` means the frozen-digest check refused after the shared preamble succeeded; an earlier mapping or migration-intent refusal remains `item-failed`. Retrying a failed token returns the failed result. Remediate the cause and start a new operation only where strict admission accepts the persisted deployment state. Operation-store corruption or a failed progress/failure commit propagates rather than masquerading as a successful transition.

Use `readFleetMigrationItemsPage(operationStore, { operationId, afterOrdinal, limit })` while running or after termination. Pages contain ordered item metadata; pass the last item's ordinal as the next exclusive cursor and stop at `done`. `abandonFleetMigrationOperation({ operationStore, operationId })` fails a running operation and its available active item as `operator-abandoned`, releases its active-operation slot, and does nothing to a terminal operation. Abandonment does not undo provider or Fleet mutations. `D1FleetOperationStore` retains terminal operations until explicit `pruneFleetOperations` and protects the latest finalized operation per kind. Custom stores choose their own terminal retention policy and must preserve active heads. Pruning never substitutes for abandoning a running operation.

### Recovery and cost boundaries

Each call holds the account's migration-kind lease. Admission and step execution also hold the active deployment's lease. It releases the deployment lease before committing operation progress. Fleet and provider state remain mutation authority; the plan and cursor sequence work. A lost progress response can therefore repeat a step against its already-committed deployment state. Admission avoids a repeated migrating write, ledgered D1 work verifies or reuses applied migrations, ordinary candidate upload can adopt by inspection, and external candidate upload repeats the same artifact. A repeated pending-topology step can write only a new timestamp; terminal convergence does not skip the remaining retirement step. Settlement delivery retains its existing at-least-once contract.

Operation progress uses wall-clock `updatedAt`, independently of the optional deployment clock. Recomposition conflicts when whole-record bytes differ; coincident timestamps can produce equal bytes and reach row comparison. A batch's own lost response or identical-object replay retains its intended bytes. Every new continue derives its transition from persisted authority rather than replaying an old update object.

The bounded path reruns the admission preamble and applicable carrier assertions for each step, where the drain runs admission once per item. It also reads every item to choose work and again to render a pending result, except when no active item remains. Without retries or failures, N items and K successful admission/step transitions require 2K full item reads, each paging at 1,000 rows: O(KN) item payload processing, or O(N²P) for comparable plan lengths P. A stale or adopted-running pending result adds one full read. The D1 progress guards also count the complete item prefix. This is a per-step provider-work bound, not a constant CPU, latency or database-row-read guarantee; size the fleet for the execution host and measure its actual runtime.

Per-call leases permit another lifecycle driver to act between steps. The frozen-target and plan fences reject incompatible phases, intent changes, backward progress and invalid carriers, but do not globally lock independent provider credentials. A READY plan can leave and re-enter the same ready discriminator between calls; later steps still converge or refuse against current state. Terminal projection checks deliberately share the drain's narrower comparison rather than revalidate every spread-through resource/history field. The [bounded migration threat boundary](security-threat-model.md#bounded-fleet-migration) describes those accepted classes and operation-store retry obligations.

Durable Object tag movement has an additional recovery limit. Continuations accept the recorded target tag or the consistent external finalized-state tag/resource pair. Fresh admission remains strict about the previous tag, just like the drain. A new operation over an external record whose tag already moved can therefore refuse on both the completed and interrupted paths. An operation that is still running after a lost progress response can resume with its continuation. A durably failed operation cannot: neither its failed token nor a new operation ID repairs the post-tag-move admission dead-end. This API provides no reset or repair operation for that state; abandonment does not supply one.

## Roll out an artifact under the execution fence

Coordinate the FlowSafe execution fence with a bounded migration of one deployment. Configure the artifact’s trusted caller epoch through `createFlowsafeWorker({ mutationEpoch })` and propagate that exact epoch to allowed mutations; never take it from a client header. Check the [trusted caller epoch requirements](deployment-reference.md#configure-the-trusted-caller-epoch) before activation, including the remaining generation-aware retention requirement.

Use the deployment’s route hostname for the [execution-fence and inventory routes](deployment-reference.md#control-plane-routes), authenticated with its maintenance admin secret. The plain Worker’s control origin does not expose those routes. Persist transition inputs and observations alongside the migration operation so a lost response can be reconciled against authoritative state.

Follow this order:

1. Read `GET /admin/execution-fence`. Persist its epoch and revision, then POST `{ expected: 'open', next: 'draining', expectedMutationEpoch, expectedRevision, advanceMutationEpoch: true }` to the same path. This compare-and-swap atomically advances the epoch and activates `requireMutationEpoch`. Configure the target artifact for the returned epoch.
2. Read the category index from `GET /admin/inventory` and sweep the categories it declares through `?category=`. Prove the drain with every `work` category empty on two sweeps taken from `draining`, at least 60 seconds apart. An empty observation has no entries and no continuation cursor; do not infer emptiness from an absent `count`. Standing categories need not be empty, and persisted idle signals remain across the migration. The [execution-fence design](do-runner-design.md#execution-fence-and-start-reservations) defines the drain and proof boundaries.
3. Start `advanceFleetMigration()` with that deployment’s record and the trusted target specification. Persist the original start input and operation ID; persist each pending token before scheduling its continuation. Keep the fence `draining` while advancing the bounded migration.
4. Require active-route attestation of the promoted artifact and settlement while the deployment lease is held. Retain the migration’s keyed, at-least-once delivery contract for attestation and settlement callbacks. Complete these checks before reopening; a candidate upload or promotion response alone does not establish completion.
5. Read the fence again and persist its counters. POST `{ expected: 'draining', next: 'open', expectedMutationEpoch, expectedRevision }` without `advanceMutationEpoch`. Verify that the returned epoch matches the activated epoch and `requireMutationEpoch` remains `true`. Reopening changes the state without disabling enforcement.

Schedule mutations encounter these refusal layers in order:

| Layer | Applies to | Refusal |
| --- | --- | --- |
| Route state gate | `create`, `update`, `resume` | `503 EXECUTION_FENCED` while `draining` |
| Storage epoch gate | Mutations including `pause` and `delete` | `409 MUTATION_EPOCH_MISMATCH`, classified `missing`, `stale`, or `future` once the requirement is armed |
| Storage state gate | `create`, `update`, `resume` | `503 EXECUTION_FENCED` when the fence does not admit authoring |

A delete with the current epoch is admitted while `draining`. After reopen, a pre-cutover artifact’s epoch is stale; a missing epoch or one ahead of the fence is also refused. For `missing`, configure the trusted writer epoch. For `stale`, replace the older artifact with the intended current artifact. For `future`, reconcile the artifact configuration with the authoritative fence and rollout record before proceeding; do not advance the fence to accommodate an unexplained caller value.

Recover response loss at the boundary that produced it:

| Boundary or failure | Recovery |
| --- | --- |
| Fence transition response lost, or `409 FENCE_CAS_CONFLICT` | Re-read and reconcile the state, epoch, requirement and revision against the persisted command. An exact retry can reuse the last upgraded command’s receipt; an intervening command invalidates it. Never blindly repeat an advance with fresh counters. The same expected counters cannot double-advance. |
| Migration response lost | Continue from the stored token. If start stopped before staging completed, replay the original start input; a token cannot reconstruct it. |
| Active-route attestation or settlement response lost | Retry with the same operation keys under the existing at-least-once contract. Reconcile the persisted migration state before reopening. |
| `503 EXECUTION_FENCE_UNREADABLE` | Restore authoritative fence readability before allowing mutation or reopening. |
| `503 SCHEDULE_MUTATION_OUTCOME_UNKNOWN` | Reconcile the schedule’s persisted outcome before retrying. A write may have occurred; this is not a clean fence refusal. |

## Switch a plain deployment to Workers for Platforms

Use `switchPlainDeploymentToWorkersForPlatforms()` only for an existing platform-authored deployment that must accept external releases without moving D1 data or Durable Object namespaces. The switch stores its intent in the canonical fleet row and holds the same `FleetStateLease` used by provision, migration, rollback, and decommission. Those lifecycle operations reject an active switch.

The switch runs these durable phases:

1. Snapshot the exact plain Worker version, D1 ID, Durable Object bindings and namespace IDs, application topology, secret names, R2 mapping and creation identity, and custom-domain owner
2. Persist the complete bridge mutation plan, including the artifact digest, append-only Durable Object history, prior and target migration tags, expected secret names, and mutation digest
3. Upload a platform-authored bridge under the same ordinary Worker name; preserve the prior application module graph and application bindings, retain every prior class export, and append only the reserved audit class migration
4. Bind the bridge to the shared outbound Worker's named `StateEgress` entrypoint and install the context-bound state credential
5. Upload and maintenance-arm the content-addressed external candidate with its target application bindings and remote Durable Object bindings to the bridge
6. Publish and verify the complete serialized host-registry target before detaching the plain custom domain
7. Verify dispatch traffic, remove every public bridge route, and atomically commit Workers for Platforms ownership in the fleet row

Rollback attaches and verifies the custom domain on the bridge before deleting the host-registry route and draining candidate traffic. It then restores the supplied prior application specification and secrets. The restored artifact keeps every append-only Durable Object export and namespace because Cloudflare cannot reverse an applied Durable Object migration. After the rollback deadline, `finalizeBackendSwitch()` replaces the bridge's fetch surface with the state-only artifact under the same script name and atomically updates its artifact snapshot. It does not change D1, R2, or any Durable Object namespace.

A finalized ordinary state bridge remains under the dedicated backend-switch provider for later provisioning, migration, and release rollback. Fleet control stores a separate finalized-state upload authorization before each possible ordinary Worker mutation, inspects the exact persisted bridge before upload, and adopts an exact committed result after a lost provider or fleet-state response. Candidate-only module changes use the trusted state artifact projection and do not upload the bridge. Trusted state migrations merge the platform profile into the persisted combined plain-and-platform history by exact tag, append only unseen class additions, use the persisted live tag as `old_tag`, and retain every recorded namespace ID. The normal Workers for Platforms backend remains dispatch-only and cannot create an ordinary per-deployment state Worker.

Use root-only `advanceBackendSwitchDecommission()` when backend-switch teardown must fit one control-plane request. The first call starts or adopts one durable operation. Each later call supplies the returned token and advances at most one release, application R2 resource, attachment-scan chunk, lifecycle group, or D1 action group. The operation binds its prior and target specifications, current desired digest, captured entry subphase, pending ordinary Worker identity, Durable Object namespace IDs, routes, releases, bridge plan, and application resources into one immutable snapshot. Every switch-intent and decommission-shell write commits atomically under the same fleet lease.

The switch advance uses the same at-least-once token rules as `advanceDecommissionDeployment()`. Stale tokens return current authority without provider work. A blocked result remains inert until exact `restart-blocked`. A matching discover and verify pass authorizes only its immediate same-lease action. The export receipt binds the D1 UUID, operation UUID, and immutable store authority, so retries after export or Fleet write loss reuse the committed artifact. D1 deletion persists its barrier before mutation, then confirms the immutable ID is absent.

Pending ordinary Worker capture reads the exact version and the authoritative secret-name inventory. It preserves Durable Object script and dispatch selectors, service entrypoints, representable R2 jurisdiction, D1 alias agreement, and provider-assigned namespace IDs. Unknown or unrepresentable binding fields fail closed. After Fleet D1 commits the snapshot, retries use that durable authority and never recapture a changed live version.

`decommissionBackendSwitch()` remains the asynchronous one-call compatibility drain. It uses the bounded engine for an active shell and for an early shell-less row with the complete capability set. A shell-less row at `decommission-export-authorized` or later stays on legacy recovery because an older writer may already have committed an operation-specific export or deletion. A wholly stripped plain pending-artifact snapshot also stays legacy unless its live carrier can reconstruct the exact authority. The bounded API refuses either ambiguous state instead of adopting it.

Before removing traffic, backend-switch teardown persists the current desired digest, the canonical set of host targets allowed by the durable lifecycle phase, ordinary bridge identity, effective bridge plan, application R2 resources, and the exact union of active, pending, migration-prior, rollback, retiring, and original switch releases. Migration permits the prior route until the candidate is armed, both prior and target routes at the publication ambiguity boundary, and only the target after publication. Publishing permits its intended pending release. Rollback and teardown preserve both active and pending routes until the final ready state commits. Each route target is bound to its snapshotted physical release and platform target. Fleet control accepts only a byte-exact member of that set when removing `HOSTS`. Each release carries its own application and physical binding topology. Fleet control records delete authorization and positive absence for each release before it deletes the bridge and verifies namespace removal. For each application R2 bucket it persists detach authorization, detached confirmation, empty authorization, empty confirmation, delete authorization, and positive absence. A retry resumes from the individual release or bucket record before D1 export and deletion. The same fleet lease fences every phase.

If you drive either bounded API from a Worker and the deployment can carry the supported application R2 maximum, use Workers Paid. The two all-bucket read groups can reserve up to 708 external subrequests. The root-only switch API can instead run from a Node control plane. Run `pnpm fleet-control:credentialed` before release because local tests cannot prove the live Workers for Platforms receiver, binding, and namespace behavior.

## Define versioned D1 migrations

Set `schemaVersion` to the final migration version. Keep the migration list ordered and append-only:

```typescript
const migrations = [
  {
    version: 1,
    sql: 'CREATE TABLE releases (id TEXT PRIMARY KEY)',
  },
  {
    version: 2,
    sql: 'ALTER TABLE releases ADD COLUMN status TEXT',
  },
];
```

Each target D1 database contains the authoritative `anchorage_fleet_migrations` ledger. The migration SQL and its ledger row commit in one D1 batch. A retry verifies the hash of the complete applied history before it advances, skips an identical committed migration, and rejects changed historical SQL. The fleet record's schema version is a resumable mirror, not the migration authority.

`migrateFleet()` runs explicit canaries first and stops on the first failure. It verifies the complete D1 ledger, records the full migration target before mutation, and applies each D1 migration before code that requires it. A superseded rollback release is recorded as retiring only after the new ready state commits; failed deletion remains retryable and visible to drift inventory.

## Audit drift and maintenance

`CloudflareProvisioningClient.collectFleetInventory()` independently enumerates ordinary Workers, their complete current deployment version sets, bindings, secret names, custom domains, and traditional Workers Routes. A ready ordinary Worker must have exactly one current version total and that version must receive 100 percent of traffic; even a zero-percent extra version is request-addressable through version overrides and therefore reports drift. Each committed immutable release stores its own exact application variable, application secret descriptor, R2, Durable Object, service, queue, and secret-name topology. Recurring inventory can therefore attest active, pending, rollback, and retiring releases without applying the current release's bindings to an older retained artifact. The client discovers every zone through the account-filtered Cloudflare API instead of accepting a caller-supplied zone list. Before route inspection or cleanup, it reads the active token policy and requires Zone Read, Workers Routes Read, and Workers Routes Write for all zones in the exact account. Missing token-policy visibility, partial zone scope, explicit denial, malformed zone ownership, or a discovery failure stops the operation. The token therefore also needs API Tokens Read. When Workers for Platforms is enabled, inventory collects the host-route and script registries, reads the authenticated paginated dispatch-script listing, validates stable fleet, tenant, and environment tags, inspects every registered dispatch script, and enumerates prefixed D1 databases, Durable Object namespaces, and fleet-owned R2 buckets. The namespace's independent `script_count` is a secondary cross-check against the paginated result, and `trusted_workers` must be exactly `false`. Fleet control repeats that exact namespace check immediately before every external script upload.

A non-default jurisdiction the account cannot access on its first page (Cloudflare answers the listing with 403, error 10003) is recorded in `unavailableR2Jurisdictions` as unavailable and holds no fleet residue; any other listing failure fails the stage.

External resource-group inventory checks the dispatch-native state script and each candidate as distinct roles under one immutable group identity. During a legacy switch rollback window, it also checks the adopted ordinary bridge. It verifies the shared D1 binding, local state namespaces, candidate remote Durable Object targets, application variables, exact secret names, application R2 bindings, exact named service and queue topology, trusted artifact and policy digests, static tenant and environment attribution, and the absence of public state routes. The backend-owned shared audit queue name is part of the persisted platform target and resource snapshot, so configuration drift cannot retarget an existing deployment. Route inventory records whether each entry came from the host registry, a custom domain, or a zone route. Fleet control reserves the owner-checked registry entry before upload so every script created through the supported client is enumerable by name. Plain-only collection makes no dispatch-namespace request.

Pass that independently collected `FleetResourceInventory` to `auditFleetDrift()`; do not derive it from fleet records. The audit works in both directions and contains an individual inspection or watchdog error so one broken deployment does not hide the rest. It reports missing, duplicate, malformed, and orphan scripts, databases, routes, Durable Object namespaces, and R2 buckets. It also reports exact application-variable, secret-name, R2-binding, route, artifact, and schema drift. Lifecycle-aware expectations distinguish an unpublished candidate, a published deployment, a retained rollback release, and resources that should already be absent during decommissioning. A deployment under an active bounded cleanup is its own reconciliation authority: the audit emits no expectation-based, orphan, or record-level findings for it — including `incomplete-provisioning` — while its cleanup intent is active, and its declared resource identities never read as orphans. A long-blocked cleanup stays visible through the record itself, in `phase: 'cleanup-advancing'` with a `blocked` step, never through drift findings. Secret-value drift remains opaque because provider inventory cannot return or hash the stored value.

The maintenance watchdog evaluates deadline expiry, SLA sweep, retention purge, and the optional background tick independently, including their last attempt and error. Plain, platform-authored Workers authenticate maintenance with the deployment's maintenance secret. An external release never receives that reusable secret. Fleet control instead signs a short-lived Ed25519 capability bound to the operation, tenant, environment, physical release script, specification digest, expiry, and nonce. The global dispatcher verifies that capability before calling `DISPATCH.get()`, and the trusted state Worker verifies it again against static deployment bindings. `ensure-maintenance` atomically consumes the nonce, while status remains replay-safe and read-only. The state Worker signs the exact result with its per-state HMAC secret, and fleet control ignores the candidate's unsigned body. The mutation request timeout must remain shorter than both the capability lifetime and the active mutation lease. The current verifier is intentionally immutable across an existing global dispatcher and deployment record: ordinary per-tenant key rotation is unsupported. Rotation requires a coordinated fleet maintenance migration or a future overlapping JWKS design.

## Audit an account under a request budget

`auditFleetDrift()` still returns the complete `readonly DriftFinding[]` array in one call, with the same finding vocabulary, order, provider interaction order, and return value. When a control-plane Worker cannot hold one full audit pass inside a single request, drive the same reconciliation logic in bounded steps with `advanceFleetAudit()`. Construct a `D1FleetOperationStore` (pass an `inventoryStore` so it can release audit pins and prune), call `start` with a caller-minted lowercase UUIDv4 operation id, the caller-supplied `records`, `staleAfterMs`, and an optional explicit `generation` (defaulting to the latest finalized R3 inventory generation), then re-enqueue only the pending token each call returns. Before it takes the operation lease, a start refuses a non-positive or non-integer explicit generation, more than 10,000 records, records whose canonical bytes total more than 16 MiB, a record whose tenant tag or environment is not a string in the deployment identifier grammar, a record above the 96 KiB staged-row byte bound, or a record outside the per-record structure bounds: plain JSON data (no `undefined`-valued properties, dates, class instances, or cycles) within depth 64, 8,192 nodes, and 4 KiB per string value or object key. Every such refusal has a fixed message and precedes every durable effect. That enumeration is not itself ordered, but the checks are: a start reaches them in one fixed sequence, so a given input always surfaces the same fixed message. The operation id, `staleAfterMs`, and an explicit `generation` are validated first, then the record count, then the array-wide null/non-object structure check. Each record is then canonicalized in array order, and the first record that fails is refused for its own structure bound, for the 96 KiB staged-row byte bound, or — once the running total crosses 16 MiB — for the aggregate byte bound. The array-wide identifier-grammar check runs last, after every byte and structure refusal. All of that precedes the lease. Within one record the structure and byte bounds are classified by first-true predicate rather than by actual cause: a record that trips both is reported as a byte refusal when a plain re-serialization exceeds the per-record bound and as a structure refusal otherwise. After the lease row is written, the foreign-kind, no-finalized-generation, and `auditClock`-sample refusals (`fleet audit auditClock sample must be a non-negative safe integer representable by Date`) write nothing else. Fleet D1 owns the operation, the stage position, and every staged row; a token is a continuation claim, not authority.

An audit start pins one finalized R3 inventory generation before staging anything and keeps that pin through completion, so the operation's findings stay interpretable against the exact generation they were computed from until the caller explicitly discards the result. Only explicit result garbage collection, terminal failure, or `abandonFleetAuditOperation()` releases the pin — finalizing an operation alone does not. When the initial probe finds the operation, a replayed start never re-resolves "latest": it reuses the persisted generation. If the probe races a concurrent creator and `startOperation()` adopts that winner, the replay can resolve "latest" locally but discards that resolution and pins the winner's persisted generation.

One call performs at most one bounded stage chunk: every global stage processes up to `maxItemsPerCall` items (1 through 2,000, default 500), and the `per-record` stage processes at most one Fleet record: at most one resolver triple, at most one provider inspection, and at most one guarded maintenance re-arm. A stale token returns the current durable result with no resolver, generation, or provider work, while a future token, an unknown operation, and a foreign-kind token all fail closed.

Two clocks feed the bounded path. The optional `auditClock` (default `Date.now`) is sampled at most once per start call, after every pre-lease refusal, after the operation probe, and — on the create branch, where the probe found nothing — after the generation to pin is resolved, which reads the inventory store only when the caller passed no explicit `generation`. Only a start that creates the operation persists the sample as `auditTimeMs`; adopted starts discard it. That persisted value drives every staleness comparison for the life of the operation, so a long-running operation does not report a record more stale than it was when the audit began. It also stamps the created run record's own `updatedAt`, the one durable write in the bounded path taken from `auditClock`; every later progress, failure, and finalize write stamps `updatedAt` from wall clock. The optional `authorityClock` (default `Date.now`) feeds only the maintenance re-arm's authority timestamp, so a first-time `authorizedAt` is stamped with call-time wall clock rather than the frozen audit time.

Durable finding rows never carry raw diagnostic bytes. The bounded engine composes each of the six sanitized template families — the five `String(error)` sites (the three resolvers, the inspection, and the re-arm) and the segmented maintenance-duty error — from a fixed template alone; `auditFleetDrift()`'s exact byte-for-byte composition remains call-local and unpersisted. Any finding detail, composed by the engine or passed through from the pinned inventory generation, that fails the non-throwing safety gate (length, control bytes, or a credential-shaped substring) persists a fixed withheld-detail fallback instead of aborting the operation.

Every finding and fact passes the same shape, vocabulary, and byte-bound codec before write and after read. If store or generation corruption violates that structure, the advance throws `fleet operation state is malformed`, writes no operation row or progress from that call, and leaves the operation running. That scope is the operation store: a `per-record` advance can already have committed its guarded maintenance re-arm to the Fleet state store before the codec or envelope check runs. Call `abandonFleetAuditOperation()` to fail that operation and release its pin.

Every finding or fact must also fit the staged-row envelope: a 16 KiB JSON-serialized payload, 4 KiB per string, and the operation codec's depth and node bounds. The coordinator checks this before either a global-stage write or a per-record commit; an excess fails the whole operation with the durable `emission-bound-exceeded` reason and releases the pin before the store sees the row. One record's whole emission set — its findings plus the cross-record ownership facts it newly claims — must additionally fit inside the one guarded D1 batch its `per-record` call commits: at most 99 rows (100 minus the one run-record update). 99 emitted rows plus the one run-record update is exactly the 100-statement budget and is accepted; 100 or more emitted rows fail. A record whose live inspection alone would emit 100 findings and facts — realistically, a record with on the order of 100 Durable Object bindings — therefore fails the whole operation with the same reason; the operation never emits a partial finding set for one record.

Read terminal audit findings with `readFleetAuditFindingsPage(store, {operationId, afterOrdinal, limit})`. Its exported `FleetAuditFindingsPage` type narrows the cursor by `done`. Pass `nextAfterOrdinal` back as `afterOrdinal` until the store reports completion. Failed operations remain readable.

The store owns the final-page signal. This reader does not compare a final page with `FleetAuditProgress.findingCount`, and it does not impose a total-page bound. A custom store must satisfy `FleetOperationStore.readOperationRowsPage`; callers should bound their own traversal when diagnosing a store that keeps reporting more pages.

Use `abandonFleetAuditOperation()` to fail a stuck running operation and release its inventory pin. Repeating abandonment on a terminal operation releases a surviving pin without changing its state.

Per-call cost is not free: every advance call that runs a stage chunk re-reads the pinned generation in full (under the requirement that an account's finalized generation materialize inside the 128 MB isolate — R3's own per-item bounds are the only cap, and its D1 reader issues two unbounded SELECTs) and re-pages the operation's accumulated `record` rows; a record-processing `per-record` call additionally re-pages the accumulated `fact` rows. The accumulated `record` and `fact` row sets materialize in that same isolate: at the 10,000-record ceiling the fact set carries the higher cap of the two — 990,000 rows read against the record cap of 10,000 — so size the isolate for all three, not for the generation alone. Each stage-running call also re-parses every re-paged `record` row through the package's structural FleetRecord ingress. Per record, that ingress performs three plain-data traversals: a bounded plain-data clone, a JSON serialization for the clone's byte bound, and a discarded `structuredClone` probe. Those traversals enforce bounds and plainness, while field shape rests on the store contract that this coordinator staged each row under canonical serialization. That three is this coordinator's own ingress only: against the D1 store each of those same rows additionally costs a `JSON.parse` of the stored payload and the staged-row codec's own bounded-plain pass, so the real per-row constant factor is higher than three. `finding` rows are never re-paged. A stale token, a `start`, and a `finalize` call read neither. The complete aggregate cost of one bounded audit spans `1 + records + Σ max(1, ⌈stage_i / maxItemsPerCall⌉)` stage-running calls, for a `maxItemsPerCall` held constant across the operation — the option is per-call, so varying it between calls changes the count: one per-record-to-finalize transition, one processing call per record, and at least one call per global stage. The sum runs over the eleven global stages rather than over distinct sources: `deployment-gaps`, `namespace-expectations`, and `r2-expected` each chunk the audited-record array independently, so that one array is walked by three separate stage runs. Every such call re-reads O(G) generation rows and re-pages and structurally re-parses O(R) accumulated `record` rows. In the records-dominated case, this is O(records) full generation re-reads, O(records²/1,000) accumulated-row page reads, O(records²) billed rows read, and O(records²) structural `FleetRecord` re-parses at three plain-data traversals each, the dominant CPU term. This checkpoint's in-memory suite measured roughly 0.25 ms per record-row re-parse and roughly 0.5 s for one `per-record` call over 1,001 accumulated rows. Multiplying the first figure by that row count accounts for about half the second; the remainder is the call's fixed cost: the pinned-generation re-read, both row pagings, and the record's own provider step. Read the per-row rate as the 0.25-to-0.5 ms band those two figures bracket rather than as a single constant, and as an order of magnitude from in-memory fakes rather than a production measurement. A late per-record call at the 10,000-record ceiling therefore spends seconds of isolate CPU re-parsing before its provider work. The per-call guarantee covers bounded provider work and bounded emission, not bounded CPU or bounded rows read.

The equivalence proof assumes that `start` accepts its inputs: at most 10,000 caller records whose canonical bytes total at most 16 MiB, each within the 96 KiB staged-row byte bound and the per-record structure bounds — plain JSON data (no `undefined`-valued properties, dates, class instances, or cycles) within depth 64, 8,192 nodes, and 4 KiB per string value or object key; every record satisfies the deployment identifier grammar, and any explicit generation is a positive safe integer. The bounded path differs from the drain in exactly four classes. First, every resolver, inspection, and re-arm failure and every multi-duty `maintenance-stale` finding persists one of the six fixed detail-template families where the drain composes the raw diagnostic. Second, any unsafe finding detail, whether composed or passed through, becomes the fixed withheld-detail fallback. Third, concurrent mutation can cause either a re-arm refusal on a Fleet reread mismatch or inspection-derived findings against later provider truth; the bounded path's typically older snapshot makes both outcomes more likely than in a drain at audit start. Fourth, `emission-bound-exceeded` (from either the staged-row envelope or the 99-row ceiling) and `generation-unavailable` are terminal whole-operation failures with no drain counterpart: `auditFleetDrift()` completes and returns its full finding array over the identical world and clocks. Every other output is proven byte-for-byte equivalent to the drain, under identical frozen worlds and clocks. The four-class claim also assumes `backendFor`, `specFor`, and `maintenanceSecretFor` are functions of record *value*: the bounded path hands them canonical snapshots rebuilt from the staged rows, never the caller's own objects, so an identity- or prototype-keyed resolver diverges from the drain in a fifth way this list does not cover.

## Inventory an account under a request budget

`collectFleetInventory()` still returns one complete `FleetResourceInventory` in a single call, with the same provider encounter order, the same finding vocabulary and order, and the same result bytes. It now drains the bounded engine in memory, which introduces one documented limitation described at the end of this section. When a control-plane Worker cannot hold that whole enumeration inside one request, drive the same engine in bounded steps with `advanceFleetInventory()`. Construct the provider seam with `cloudflareFleetInventoryContext(client)` and a `D1FleetInventoryRunStore`, call `start` with an operation id and the same options `collectFleetInventory()` accepts (now exported as `CollectFleetInventoryOptions`), then re-enqueue only the pending token each call returns. Fleet D1 owns the operation, the stage position, and every staged row; a token is a continuation claim, not authority. One call performs at most one provider stage chunk, bounded by `maxProviderRequests` (an integer from 9 through 1,000) and `maxStagedRowsPerChunk` (1 through 2,000, default 500). A stale token returns the current durable result with no provider request, while a future token, an unknown operation, and a foreign active operation fail closed.

The final call returns a `FleetInventoryGenerationRef`, not the inventory. Read the rows back with `readFleetInventoryGeneration(store, generation)`, which materializes the same `FleetResourceInventory` shape `auditFleetDrift()` expects. Only a finalized generation is readable: a staging, failed, or count-divergent generation is structurally unreadable. The latest finalized generation reads without a pin; any older generation must be pinned first with `pinGeneration()`, because `pruneInventoryGenerations()` deletes only finalized-or-failed, non-latest, unpinned generations.

Direct `FleetInventoryLease.commitChunk()` callers must retain the run's account, generation and options, and supply unique row and fact keys. Retry an uncertain commit with the same intended record and payload bytes. Read persisted progress before composing a new transition; an equal revision alone does not establish that the intended transition committed. Conflicting immutable payloads refuse the batch instead of replacing earlier observations. Database errors propagate to the trusted caller.

Retry the original continuation after an interrupted finalization. The coordinator repairs the matching finalized head before reading the generation. Direct failure callers can retry the same `failRun()` request to release an interrupted head; neither repair clears a newer operation. Pruning retains the active terminal generation so that repair can finish. Await dependent mutations made through one lease handle; independent calls can overlap. A pin that loses a race with reclamation refuses, while an admitted pin protects the generation from pruning. A partial prune remains retryable and cannot gain a new pin over missing data. Pinning can scan the generation in D1; account for that database work when sizing the control plane. Historical generations still require a pin when they are no longer latest.

Two limitations are deliberate. First, a generation is a point-in-time-per-stage snapshot, not a globally consistent one: a resource that changes between stages — a script deleted after the script listing but before its detail read — is recorded exactly as the single-call drain surfaces it, through the same `incomplete-deployment` finding. This is the guarantee `collectFleetInventory()` has always given. Second, durable findings never echo transient provider text. The two sites that previously interpolated a provider error store the fixed details `registered script '<name>' could not be inspected` and `plain Worker '<name>' could not be inventoried`; the transient text stays call-local, which is why `collectFleetInventory()` can still compose today's exact bytes while the durable row cannot.

One compatibility limitation follows from that shared engine, and it applies to `collectFleetInventory()` as well as to a bounded run. A stage chunk is bounded by `maxProviderRequests`, whose maximum is 1,000, and six stages carry no resumption cursor or ordinal: `registration-postprocess`, `custom-domains`, `zone-authority`, `route-claims`, `d1-databases`, and `do-namespaces`. Those six must finish inside one chunk, so exhausting the budget there is a refusal rather than partial progress. `collectFleetInventory()` supplies the maximum 1,000 to every chunk, which means an account whose single non-resumable stage needs more than 1,000 provider operations now refuses where the previous single-pass enumeration completed under the 10,000-item collection bound. In practice `route-claims` is the first to reach it: it re-reads the custom domains, the zone list, every zone's route pages, and one identity read per prefix-matching plain Worker, so roughly 1,000 prefix-matching plain Workers in one account is the threshold. An operator sees `fleet inventory stage 'route-claims' cannot complete one chunk within its provider request budget` (naming whichever stage saturated) instead of an inventory; nothing is written and no partial result is returned. The 9-through-1,000 budget is a deliberate bounded boundary, so there is no unbounded mode: split such an account across narrower `scriptNamePrefix` values.

Host-routing KV key names are untrusted input in the enumeration. An over-length or credential-shaped key name refuses the run outright. A key name that is merely unprintable or base64-shaped does not: the run records a `malformed-script-registration` finding that names the key by its zero-based listing ordinal — `script inventory key at ordinal <n> has an unsafe name` — instead of echoing the bytes. The accepted trade-off is attribution: that finding is positionally attributable but does not carry the offending name, so resolving it means listing the KV namespace yourself.

## Decommission without losing the export

Use `advanceDecommissionDeployment()` from a Queue-driven control-plane Worker for request-bounded normal decommissioning. Call it first with `start`, then re-enqueue only the pending token returned by each call. Fleet D1 owns the operation and scan progress. A Queue message carries a continuation claim, not authority.

At-least-once delivery is safe. A stale token returns the current durable result without repeating provider work. Future tokens and tokens for another deployment or operation fail closed. A blocked result remains inert until you remove the reported attachment and submit exact `restart-blocked` with its current token.

Each call performs at most one bounded scan chunk. Only an exact matching verify may consume that scan result immediately through one resource action under the same live lease. Evidence never becomes a reusable deletion certificate. A call without matching verify performs at most one lifecycle or resource action group.

Queue-driven bounded decommissioning requires Workers Paid. Each of the two all-application-R2 read groups can reserve up to 708 external subrequests for the supported maximum of 118 application buckets. That exceeds the Free plan limit of 50 subrequests per request and remains below the Paid default of 10,000. These limits were checked on 2026-08-30 in the [Cloudflare Workers limits](https://developers.cloudflare.com/workers/platform/limits/).

Before the first D1 scan or export, Fleet D1 persists one immutable receipt authority. The receipt identity combines that authority, the canonical lowercase D1 UUID, and the decommission operation UUID. Retries after receipt commit or Fleet write loss converge on the same artifact. An authority change or byte collision preserves the committed winner and fails closed.

Custom bounded backends must expose both `databaseExportReceiptAuthority` and `exportDatabaseReceipt()`. The export method must return descriptor-safe plain data with the matching database ID, location, positive size, and lowercase SHA-256 digest. Fleet control accepts safe extra plain-data fields but strips them. It rejects prototype-bearing instances, accessors, proxies, and malformed required fields.

The asynchronous one-call `decommissionDeployment()` compatibility path drains this bounded engine for every row with a normal decommission shell. It also selects bounded execution for an early shell-less row with the scan capability and at least one receipt member, so a malformed partial pair fails closed. The legacy asynchronous one-call path remains for shell-less late D1 or terminal rows and for other shell-less rows that lack the complete scan-plus-receipt capability.

Normal decommission persists these destructive phases:

1. Require every fleet-owned application R2 bucket to be empty before any traffic mutation
2. Remove traffic, prove zero ingress, and persist `traffic-removed`
3. Reassert zero ingress and R2 emptiness on every `traffic-removed` retry
4. Revoke both deployment credentials and delete the active and retained candidate Workers
5. Revoke the state script's credentials and delete the state script; a legacy switch also removes its adopted bridge after namespace-safe teardown
6. Scan every current ordinary Worker version and dispatch script for application R2 attachments
7. Delete every empty fleet-owned application bucket and confirm its absence
8. Stream the D1 export into configured durable storage
9. Persist its durable location, SHA-256 digest, and byte count
10. Persist database-deletion intent, delete the database, confirm absence, and persist `decommissioned`

That terminal row is retained rather than deleted: it keeps the complete record — the export location, SHA-256 digest, and byte count, and the application resource entries at `deleted` — which `forceDecommissionDeployment()` clears and a re-provision of the same slug replaces, as described under [Provision a deployment](#provision-a-deployment). `decommissionDeployment()` replays that export for a late retry while the terminal row is the stored row; once a re-provision has replaced it, the same call is a new decommission of the replacement, so read the export from its retained location before reprovisioning the name, or carry an in-flight decommission through `advanceDecommissionDeployment()`, whose token carries the operation identity.

Zero ingress covers every surface owned by the workload. Plain Workers must have no custom domain or zone route, and fleet control explicitly disables and rechecks workers.dev and preview URLs. Workers for Platforms deployments must have no `HOSTS` record. A backend switch checks `HOSTS` plus every ordinary bridge ingress surface. If ingress drift or a late R2 write appears after removal, fleet control preserves the traffic-removed state and every script, credential, platform resource, and bucket. It does not restore traffic. Remove the unexpected ingress or evacuate the bucket directly, then retry.

An attached or nonempty application R2 bucket blocks deletion. Fleet control never purges application objects automatically because no generic export format can preserve application semantics. The backend requires positive persisted ownership, exact provider identity, complete paginated emptiness inspection, and positive absence after deletion.

An export failure, empty export, size mismatch, or missing integrity metadata prevents database deletion. Both provider backends require a `DurableDatabaseExportStore`; the store must return the byte count and SHA-256 of the committed artifact, which the backend compares with the exported stream. The Wrangler backend writes a mode-0600 scratch file, streams it to the durable store, and removes the scratch file before returning only the store location. `FileSystemDatabaseExportStore` computes that integrity while atomically publishing a verified local artifact for the credentialed scratch-account gate. Its configured filesystem is responsible for the resulting artifact's durability; production control planes should implement the same narrow interface with durable object storage such as R2. Cleanup and decommissioning resolve the database by its persisted immutable ID, validate its name and deployment sentinel when present, and treat only a positive provider 404 as absence. Plain-worker teardown deletes that ID through Cloudflare's D1 REST API, then re-reads the same ID before it advances; it does not pass a UUID to `wrangler d1 delete`, whose operand is a database name or configuration binding. A custom `PlainWorkerRouteApi` must supply `getDatabase` and `deleteDatabase` for this destructive step. Before export and again before deletion, the backend scans every version in each current ordinary Worker deployment, including request-addressable zero-percent candidates, and every dispatch script account-wide for an exact D1 attachment; an unregistered or unrelated attachment fails closed. Historical versions outside the current deployment are excluded. A retry therefore converges after a crash between database deletion and the final state write without following a reused name to another database. Decommissioning retains the persisted platform-resource snapshot until both trusted Workers are positively absent, and never deletes D1 while a state Worker can still hold its binding.

## Force decommission when retained inputs are lost

The named root export `forceDecommissionDeployment()` removes an ordinary deployment when the trusted host cannot reconstruct its `DeploymentSpec`. It is separate from the curated Worker factory. Call the existing API with the persisted tenant and environment key:

```typescript
await forceDecommissionDeployment({
  backend,
  store,
  tenantTag,
  environment,
  options: { audit },
});
```

The function always acquires `store.withDeploymentLease()` before it reads the ledger. An absent deployment succeeds without a provider mutation. A terminal `decommissioned` record also succeeds, removes the retained ledger row, and emits no audit event.

A concurrent provision or decommission receives the same lease-acquisition error as `decommissionDeployment()`. A `database-reserved` row has not authorized provider creation, so force decommission removes that reservation without a provider call.

The force path uses persisted resource identities only. It never fetches an artifact, rebuilds a specification, reads `FLEET_SPEC_DIGEST`, or performs version-ownership attestation. A `database-create-authorized` row still contains a synthetic reservation ID even if D1 creation succeeded before a state-write crash. Force decommission retains that row and fails closed because the route API cannot prove a unique exact provider ID from the synthetic value. Resume the spec-aware provisioning or cleanup path to reconcile that state.

`WranglerLoopBackend` implements the required `forceDecommissionStep` seam through its private route API. It performs these replay-safe stages:

1. Detach every custom domain served by the persisted script, disable workers.dev and preview URLs, then inspect the Worker again and refuse to continue while any public ingress remains
2. List and delete every current Worker secret, then require an empty secret inventory
3. Resolve D1 by the persisted immutable ID, require the persisted database name, delete through the exact-ID REST API, and confirm absence

Each provider mutation receives the active lease fence. `WranglerLoopBackend` requires route API implementations of both `getDatabase` and `deleteDatabase` before the first D1 lookup. The exact-ID lookup, deletion, and confirmation all run within that route fence; force deletion never falls back to Wrangler. The ledger persists `decommissioning`, `traffic-removed`, `credentials-revoked`, and `database-deleting`, so an interrupted call repeats the incomplete idempotent stage.

Provider 404 responses mean the resource is already absent. After D1 is absent, the function persists `decommissioned`, emits `DecommissionAuditEvent` with `forced: true`, and calls `lease.delete()`. If initial audit delivery fails, the terminal row remains. A retry deletes it without repeating provider mutations or redelivering the event. Normal decommission uses the same optional event with `forced: false`.

Force decommission does not delete the ordinary Worker script, application R2 buckets, or control-plane retention data. It removes the deployment’s ingress, live Worker secrets, database, and fleet ownership record. After the call returns, the host deletes its separate retention row and revokes its gateway key. Workers for Platforms fails closed unless its backend implements equivalent spec-free primitives; its dispatch route and trusted-resource topology cannot use the ordinary Worker route API contract.

Every backend operation that can issue a non-GET Cloudflare request receives the active `ExternalMutationFence`. `CloudflareProvisioningClient.withMutationFence()` carries that fence across queued SDK calls and renews ownership immediately before each provider write. The client rejects unfenced writes and request timeouts that are not shorter than the lease lifetime. The Wrangler backend applies the same rule to its process-tree timeout and rejects an invalid composition before spawning the mutation command. A lease takeover therefore stops the next provider mutation, including retries and cleanup sub-steps.

`D1FleetStateStore` memoizes only a successful or currently in-flight schema bootstrap. A transient D1 failure clears the memo so the same store instance can retry. The additive `backend_switch_intent` upgrade verifies the exact resulting nullable `TEXT` column and accepts only a verified duplicate-column race between concurrent cold replicas; every other migration failure propagates.

Cloudflare documents a 1,200-request-per-five-minute client API limit per user or account token, cumulative across callers. `CloudflareProvisioningClient` therefore requires a `CloudflareApiRateCoordinator`. Production uses `D1CloudflareApiRateCoordinator` over one shared direct Workers `D1Database` binding and an explicit nonsecret `quotaScope` identifying that provider quota. The coordinator calls only the binding's `prepare()` and `batch()` methods. Its runtime guard rejects objects without that interface, but JavaScript cannot prove whether a structurally compatible object is a direct binding or a remote facade. The trusted host must enforce the direct-binding requirement because remote coordination queries would consume the same Client API quota being protected. Independently constructed coordinators with the same binding and scope atomically share a rolling cap of 1,100 requests per five minutes across replicas. A non-Worker control plane must use a separately deployed coordinator service rather than a remote database adapter. Every SDK request, retry, pagination page, authenticated manual request, and signed export download reserves capacity before the network request. Coordinator failure fails closed. Ambiguous transport failures retain their reservation. Never derive the scope from, persist, or log the raw token. User-scoped credentials used across accounts must deliberately reuse one scope. The remaining 100 requests are reserve for dashboard and other out-of-band traffic; fleet control cannot count or guarantee room for that traffic.

## Clean up failed provisioning with durable receipts

`cleanupDeploymentArtifacts()` removes an owned prepublication deployment without a database export. It drains the same bounded engine that `advanceCleanupDeployment()` exposes for Queue-driven control planes: call `start` first, then re-enqueue only the pending token each call returns; a blocked result stays inert until an exact `restart-blocked` with its current token. Fleet D1 owns the operation, its bounded attachment-scan progress, and the terminal receipt; a token is a continuation claim, not authority. One call performs at most one bounded scan chunk or one action group. Stale tokens return the current durable result, while future tokens, tokens for another deployment, and decommission tokens fail closed. An active cleanup intent of either authority resumes through `start`; a partial drain leaves the durable intent for retry.

No-export database deletion is admissible only when the deployment provably never authorized a candidate invocation. Every new record carries a durable invocation-authority carrier from its first persisted write, and every candidate-invoking dispatch — an external candidate upload, the first maintenance request, a version override, or a promotion — commits an authorization timestamp durably before the provider call. A rejected or unacknowledged authority write aborts before dispatch, and a lost provider response counts as possible execution. Trusted platform-authored deployments therefore keep no-export cleanup through `worker-deployed`. Rows with an authorized carrier, Workers for Platforms and external-artifact candidates (every current candidate binds the deployment database), rows carrying external staging evidence, and legacy carrier-less rows at upload-ambiguous or later phases all refuse with a fixed message that names export-backed decommissioning as the remedy. Legacy carrier-less rows at phases that could not yet have dispatched an upload stay eligible, and the eligibility re-check runs again immediately before the database-deletion group.

A completed cleanup atomically persists an immutable operation-keyed terminal receipt, releases the deployment's ownership claims, and deletes the fleet row in one D1 batch. The receipt records the admitted phase, the authority (`manual-cleanup` or `provisioning-rollback`), the disposition (`reservation-cleared` or `prepublication-owned-no-export`), and provider-text-free evidence. Receipts survive reprovisioning of the same key and force decommission, so a delayed token converges on its receipt instead of touching a new row. Read one with `readCleanupReceipt()`; prune explicitly with `pruneCleanupReceipts({ completedBeforeMs, limit })`, which uses the database-assigned completion time, a stable order, and an integer limit from 1 through 1,000. Pruning invalidates delayed tokens for exactly the pruned operations.

A failed provision whose rollback admits the engine is durably `cleanup-advancing`: `provisionDeployment()` refuses to resume that row and directs to cleanup; complete the cleanup to its receipt, then reprovision fresh. `provisionDeployment({ failureCleanup: 'bounded' })` performs at most one bounded advance during rollback and surfaces the resumable outcome through `ProvisioningError.cleanup`. `forceDecommissionDeployment()` refuses during an active bounded cleanup — after remediation, `restart-blocked` is the only resolution for a blocked operation. On capable stores, force releases the deployment's current ownership claims with its terminal row delete; a legacy lease implementation without `deleteReleasingClaims` keeps tombstone claims through the plain row delete. Force remains receipt-free and evidence-free, and it does not delete the ordinary Worker script or application R2 buckets, so confirm the residual physical resources are removed before reprovisioning the same names. Provisioning fails closed on what remains rather than adopting it. A terminal row that retains an application resource refuses re-provision by that name and directs to `forceDecommissionDeployment()`. A re-provision over a retired terminal row proves the reserved database name free before it claims the row, so that refusal leaves the record and its export triple intact. The plain-Worker backend refuses to upload over a surviving script whose deployed versions bind another tenant, environment, or database.

## Deploy the Workers for Platforms control plane

`provisionPlatformPlane()` requires a `PlatformPlaneStateStore`. `D1FleetStateStore` implements it with permanent ownership claims for the account-scoped dispatch namespace, three ordinary Worker names, host-routing KV namespace, audit queue, and optional dead-letter queue. It also holds a renewable database-time lease over that exact resource set. A crashed owner can resume after lease expiry, but another platform-plane identity cannot claim any overlapping resource. The lease fences every mutation, covers the initial ownership inspection, and remains held through a final whole-group reinspection.

The provisioner owner-checks three distinct ordinary Worker names against a stable platform-plane identity before mutating them, then uploads the Workers in dependency order. It creates or reuses only a dispatch namespace that attests `trusted_workers=false`. It converges the audit queue's sole consumer by consumer ID whenever the script, dead-letter queue, batch size, concurrency, retry count, or batch wait differs, then re-reads and attests the complete consumer configuration. The outbound and audit Workers have workers.dev, preview URL, custom-domain, and account-wide zone-route access removed and re-inspected before provisioning succeeds:

- The outbound Worker verifies a control-plane-authored policy identity and digest bound to the exact tenant and environment, applies that deployment's hostname allowlist, denies redirects, and logs the complete attribution
- The shared audit Worker consumes the audit queue and exports newline-delimited JSON to the configured security information and event management (SIEM) endpoint
- The dispatch Worker maps a hostname through KV, validates the deployment policy metadata stored with that mapping, applies CPU and subrequest limits, and invokes the user Worker with the same outbound policy context. Missing, malformed, cross-deployment, or digest-mismatched policy data fails closed. Its maintenance-only path can invoke one exact unpublished script, but it cannot expose that script's application routes

Platform-authored state scripts may hold the declared shared queue producer. External candidates never do. An external candidate sends an authenticated event to its remote `AUDIT_PROXY` Durable Object binding. The fixed singleton in the trusted state script caps and validates the body, discards caller-selected attribution, and wraps the event in a canonical envelope using static deployment identity. It marks the event semantics as untrusted and alone holds the backend-owned `AUDIT_QUEUE` producer. Only the envelope attribution is authoritative. Candidate-selected action, decision, resource, and detail remain claims. The single control-plane consumer reads that distinction. User Workers never expose `scheduled()` or `queue()` handlers.

Workers for Platforms outbound handlers do not cover Durable Object network calls. The fleet backend therefore binds each platform-authored state script to the one shared outbound Worker's named `StateEgress` entrypoint. `createStateEgressFetch(env)` overwrites every reserved attribution and credential header from static state bindings. The shared entrypoint compares that context with the canonical `HOSTS` record, verifies the credential digest in constant time, enforces the host policy, and strips reserved headers before the origin request. External candidates receive neither the service binding nor the credential. They cannot export Durable Object classes and bind only to stable platform-owned state.

## Run paid namespace conformance

The paid gate verifies the provider behavior that local tests cannot reproduce. Treat a passing run as a mandatory release condition for changes to Workers for Platforms provisioning, bindings, limits, migrations, routing, or FlowSafe runtime integration.

Build one external candidate and two trusted state artifacts. [`packages/agent-starter`](../packages/agent-starter/README.md#submit-it-as-a-workers-for-platforms-artifact) builds all three and ships a ready operator configuration; use it unless you are authoring a different artifact. Otherwise start from [`credentialed-conformance.example.json`](../packages/fleet-control/scripts/credentialed-conformance.example.json). Keep `contractVersion` and `platformProfile.runtimeContractVersion` at `1`. Configure exactly two tenant tags, the backend-owned audit queue, positive CPU and subrequest limits, one application variable, one application secret binding, at least one application R2 bucket, and allowed and denied upstream URLs. Do not put credentials or secret plaintext in the file.

The runner validates in two stages. Structural validation checks the versioned configuration, required environment values, and private-key shape before it reads artifacts or imports fleet code. After it constructs both releases and both trusted profiles, the production specification, secret, external-profile, migration, route, date, and canonical JWK validators run before Cloudflare client or backend construction. Either stage fails closed without a provider request.

The v1 state profile must export the original FlowSafe Durable Object classes. The v2 profile must repeat the complete v1 migration history and append the migration for `conformance.newDurableObjectBinding`. Both artifacts must own their classes locally, use the candidate's D1 database, relay audit events through the trusted audit proxy, and send Durable Object network calls through `createStateEgressFetch(env)`.

### Implement the artifact contract

Each candidate must serve a JSON action endpoint at `conformance.httpPath`. Requests contain `contractVersion: 1`, `action`, and the action fields below. Every response must contain only the documented fields and must repeat `contractVersion: 1` and the exact action.

| Action | Request fields | Required response evidence |
| --- | --- | --- |
| `application-bindings` | `nonce` | configured `variableName` and `variableValue`; `secretName`; HMAC-SHA-256 of the nonce in `secretHmacSha256`; `secretPlaintextExposed: false` |
| `audit-proxy` | `nonce` | matching `nonce`; `accepted: true` after the trusted audit proxy accepts the event |
| `connector-egress-allowed`, `connector-egress-denied` | `url` | `allowed: true` or `denied: true`; actual `upstreamStatus` from `ConnectorRuntime` traffic |
| `state-egress-allowed`, `state-egress-denied` | `url` | `allowed: true` or `denied: true`; actual `upstreamStatus` from a trusted Durable Object using `createStateEgressFetch(env)` |
| `cpu-control` | none | `completed: true` after bounded CPU work |
| `cpu-over-limit` | none | Cloudflare terminates the request with `conformance.cpuOverLimitStatus`; no success body can satisfy this action |
| `r2-write` | randomized `key`, `value` | matching `key`; `written: true` after the application binding stores the object |
| `r2-read` | `key` | matching `key` and exact `value` from the application binding |
| `r2-delete` | `key` | matching `key`; `deleted: true` after candidate deletion |
| `r2-absent` | `key` | matching `key`; `absent: true` after a provider read returns no object |
| `state-marker-put`, `state-marker-get` | `marker` | exact `marker` stored in or read from the original state namespace |
| `state-new-class` | `nonce` | matching `nonce`; `stored: true` from the class added by the v2 migration |
| `flowsafe-start` | `effectNonce` | `runId`, `approvalId`, integer `revision`, `status: "pending"`, and `effectCount: 0` after suspension |
| `flowsafe-approve` | `runId`, `approvalId`, `revision` | matching identifiers, `status: "approved"`, `resumed: true`, and `effectCount: 1` |
| `flowsafe-status` | `runId` | matching `runId`, `terminalD1: true`, and `effectCount: 1` from durable D1 state |
| `flowsafe-replay-decision`, `flowsafe-replay-resume` | `runId`, `approvalId`, `revision` | HTTP 409, matching `runId`, `rejected: true`, and `effectCount: 1` |

The WebSocket endpoint at `conformance.webSocketPath` accepts its request envelope as the first client frame. `nonce-echo` returns only the version, action, and matching nonce. `flowsafe-approval-update` also receives the run and approval identifiers and must return the pending approval's matching identifiers, revision, and status.

The runner starts the FlowSafe run and observes its pending WebSocket update on v1. It then uploads v2, verifies the old state marker and namespace IDs, exercises the new class, and only then approves the suspended request. A pass therefore proves update, approval, resume, one effect, both replay rejections, and terminal D1 state as one ordered flow.

Set these environment variables:

- `CLOUDFLARE_ACCOUNT_ID`
- `CLOUDFLARE_API_TOKEN` with API Tokens Read, Zone Read, Workers Routes Read, and Workers Routes Write for every zone in the account, plus account-scoped Workers Scripts Edit and D1 Edit, and Workers R2 Storage Edit when application R2 is enabled. [Security threat model](security-threat-model.md) lists the route families the client calls with this token.
- `FLEET_CONFORMANCE_CONFIG`
- `FLEET_MAINTENANCE_CAPABILITY_PRIVATE_JWK` containing the fleet-private Ed25519 signing JWK
- `FLEET_STATE_EGRESS_ROOT_SECRET` containing the shared state-egress derivation secret
- `FLEET_CONFORMANCE_APPLICATION_SECRET` containing disposable plaintext for the configured `applicationSecretBinding`

Keep only the canonical public JWK in `platformProfile.maintenanceCapabilityPublicKey` inside the JSON configuration. Keep the private JWK, state-egress root secret, and application-secret plaintext out of that file and logs. The runner computes the application secret descriptor at runtime and persists only its SHA-256 digest and binding name.

Run the credentialed gate:

```bash
pnpm fleet-control:credentialed
```

The gate creates two deployments with distinct application R2 buckets, rejects a cross-tenant D1 sentinel restamp, and checks each candidate's exact D1, application variable, application secret name, and fleet-owned R2 topology. It performs a same-name trusted-state upload with `keep_bindings`, proves that the original maintenance secret still signs a valid receipt, and immediately repairs the durable artifact-version snapshot through normal lifecycle convergence.

Before final teardown, the candidate leaves one randomized R2 fixture present. Decommission must reject it before phase advance, route removal, credential revocation, or Worker deletion. The candidate then deletes the fixture, proves provider absence through `r2-absent`, and retries. The gate re-hashes each retained export against its recorded SHA-256 and byte count, then requires zero registered scripts, namespace scripts, D1 databases, Durable Object namespaces, R2 buckets, or host routes under the test prefixes. Run it only in a scratch account with a Workers for Platforms subscription.

After both Workers for Platforms deployments decommission, the runner derives the account's Workers subdomain. It reuses the first released route for a platform-authored plain Worker. This lane uses the configured v1 trusted-state artifact, `WranglerCommandRunner`, `WranglerLoopBackend`, `CloudflareProvisioningClient`, and the same `FileSystemDatabaseExportStore`. It adds no configuration fields. Provide Wrangler `>=4.118 <5` in the host environment.

A backend wrapper records a valid, nonempty `wrangler versions list --json` version-ID set immediately before control-secret revocation, after revocation, and before Worker deletion. Secret deletion may add Worker versions, and Wrangler's ten-entry rolling window may remove earlier IDs from later observations. Decommission must still reach `decommissioned`, and the gate re-reads the exported database's immutable ID through Cloudflare to prove absence. Fleet Control uses exact persisted artifact-ID membership as the pre-mutation gate in traffic removal. Before secret mutation and Worker deletion, it separately resolves the persisted artifact with `wrangler versions view`, which is not limited to the ten entries returned by `versions list`, and verifies that every deployed version has the persisted tenant, environment, database, specification, schema, and ingress identity. This live check accepts provider-created version IDs. Deletion also validates ingress and the resource footprint, then verifies full Worker absence. If the proof fails after control-secret deletion begins, the runner validates the same live teardown identity before removing that exact uniquely suffixed Worker and resuming normal database and state cleanup.

## Run the direct-API credentialed proof

Use the repository's direct-API lane to verify ordinary Worker provisioning, migration interruption and resumption, execution-fence proofs, and teardown. The offline acceptance runs the real runtime through a fixture provider and local workerd, starting from confirmed bootstrap receipts. It exercises bootstrap revalidation and teardown; resource creation and live provider behavior require the credentialed lane. No live acceptance is established until you supply credentials and run it against an account.

Start with the [direct configuration shape](../packages/fleet-control/scripts/direct-credentialed-conformance.example.json). Supply the reference and tenant artifacts and their digests, an owned hostname, a disposable resource prefix, and explicit request and invocation limits. Keep credentials outside the configuration. For the deployment protocol, follow [Roll out an artifact under the execution fence](#roll-out-an-artifact-under-the-execution-fence).

Run this lane on Linux from a repository checkout. It uses a filesystem lock and stores the journal and evidence under `.direct-conformance/<resourcePrefix>/` beside the configuration file. Preserve that directory and the configuration bytes when resuming.

Set these environment variables:

| Variable | Required for | Value |
| --- | --- | --- |
| `FLEET_DIRECT_CONFORMANCE_CONFIG` | Preflight, run, resume | Path to the configuration |
| `CLOUDFLARE_ACCOUNT_ID` | Run, resume | Account identifier |
| `CLOUDFLARE_API_TOKEN` | Run, resume | Provider token; the residual scan lists the account's queues, so it must permit that read |
| `FLEET_DIRECT_CONFORMANCE_INVOKE_SECRET` | Run, resume | Reference-worker invocation secret |

Values must be nonempty, without surrounding whitespace or control characters. The runner does not load `.env` files or discover a Wrangler login. Help needs no configuration or credentials.

Validate the local configuration and artifacts before supplying credentials. The default mode is `--preflight`; it constructs no SDK client, takes no run lock, and writes no evidence. The workspace script builds the package before invoking the entry:

```bash
pnpm fleet-control:credentialed:direct -- --preflight
```

After a build, the entry avoids another build for local preflight:

```bash
node packages/fleet-control/scripts/direct-credentialed-conformance.mjs --preflight
```

Start a new run with `--run`. When it exits with `3`, run `--resume` in a fresh process using the same configuration and credentials:

```bash
pnpm fleet-control:credentialed:direct -- --run
pnpm fleet-control:credentialed:direct -- --resume
```

Modes are mutually exclusive. Use `--help` for usage. Live modes require built package output and reject an invocation budget below the scenario floor before acquiring a lock or contacting the provider. Resuming an in-flight scenario spends a bootstrap control read as well as scenario invocations; phase ceilings and remaining-phase reserves also constrain progress.

| Exit | Meaning | Action |
| --- | --- | --- |
| `0` | Cleaned, or successful preflight/help | Inspect the summary and, for a live run, evidence |
| `1` | Failed, outcome unknown, or run-state refusal | Inspect the refusal and retained identities |
| `2` | Invalid usage, environment, configuration, or live-mode admission | Correct the named input |
| `3` | Restart required | Resume in a fresh process |
| `4` | Resources retained | Use the recorded facts for recovery approval |
| `5` | Evidence failed, or the summary line is withheld because it would contain a credential | With a summary, check `evidenceWritten`; with no output at all, read `evidence.json` directly: its `exitCode` and `status` are the run's own |

A successful complete teardown makes a later resume evidence-only, with no provider requests and a null `teardownCall`. A recorded teardown refusal re-observes residuals and retains resources instead of advancing into deletion. Pending invocation or bootstrap mutations refuse automated continuation with `outcome-unknown`: inspection validates the journal binding under the same lock, reports retained identities, and writes evidence without mutating the journal. Pending teardown work can resume reconciliation. After the ingress probe, the client re-sends identical requests under one reservation within the shorter of 120 seconds or the invocation timeout: read-only actions retry transport failures and non-contract answers; mutations retry only unmarked 404 text pages. The first send and any answer whose contract headers have arrived are bounded by the invocation timeout alone. Answer failures report a fixed `detail`: `platform-page`, `transport-failure`, `non-contract-answer`, or `delivery-window-expired` when read-only delivery exhausts that window. Concurrent callers fail with `lock-unavailable`; an existing run refuses `--run` with `run-exists`.

Give `evidence.json` to the recovery approval. Its allowlist projects configuration and artifact digests, versions, times, resume and invocation counts, dispatch classification, scenario failures and proofs, teardown receipts and residual counts, the current teardown call, and retained resource identities. Account and zone identifiers are represented by hash suffixes; cost remains `unknown`. The journal retains residual names that evidence omits. Top-level `status` describes cleanup disposition, while `scenario.failure` describes the scenario outcome; `teardownCall` records the current call separately from durable teardown state.

The writer scans decoded string values and serialized bytes for credentials and forbidden literals. It writes with mode `0600`, verifies a temporary file by reading it back, and replaces the artifact atomically before syncing the directory. A failure before replacement leaves an older artifact untouched and reports `evidenceWritten: false`; a directory-sync failure after replacement reports `true` with durability unconfirmed. Summaries and refusals exclude raw provider errors, credentials, headers, and bodies.

Treat residual inventory according to its recorded scope. The scan lists scripts, domains, routes, and queues as single pages, and each records `exhaustive` from the provider's `result_info`: `true` when the provider corroborated the page, `false` when it sent no attestation. The transport refuses a response whose `result_info` contradicts a complete page. `isSettled` asserts a prefix-scoped zero residual on each recorded surface, and a global zero residual only under `disposableAccount: true`, both from the recorded counts; the global bucket count covers the `default` jurisdiction. A journal written before this version carries no queues surface and is refused on resume, so start a new run. Offline results do not establish live resource creation, live account cleanup, or recovery authorization.

## Preserve the control-plane boundary

Keep these constraints in every operator surface:

- Store API tokens outside tenant Workers
- Give every production `CloudflareProvisioningClient` a `D1CloudflareApiRateCoordinator` backed by one shared direct Workers D1 binding. Use the same explicit nonsecret `quotaScope` for every replica and account sharing a Cloudflare user/account-token quota. The trusted host must enforce that the structurally typed database value is the direct binding; never substitute a REST-backed D1 adapter
- Keep the durable API cap at 1,100 Anchorage-originated requests per rolling five minutes. The 100-request reserve does not make unrelated dashboard or API traffic observable
- Use `ProcessLocalCloudflareApiRateCoordinator` only for local tests and the one-process credentialed runner; it is not replica-safe
- Resolve deployment identity from trusted fleet state, not request content
- Treat the state-store lease and unique route constraint as ownership authority; Workers KV is a derived, eventually consistent publication surface
- Keep application secret values at the trusted invocation seam and treat provider inventory as name-only attestation
- Bind only fleet-owned application R2 buckets, and require attachment absence plus emptiness before deletion
- Reject application KV; reserve the shared `HOSTS` namespace for control-plane routing
- Keep the dispatch namespace untrusted for external artifacts
- Keep external release migrations expand-only while a rollback candidate is retained
- Treat the paid namespace conformance command as a release gate
