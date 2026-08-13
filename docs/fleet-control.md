<!-- Content type: Reference -->

# Provision physically isolated deployments

Fleet control provisions one D1 database, fleet-owned application R2 buckets, and one isolated Worker resource group per project environment. Platform-authored deployments use one ordinary Worker. Fresh external deployments use two Workers for Platforms user scripts: one stable platform-authored state script and one content-addressed candidate. They consume no ordinary Worker slots per deployment. Use the `@proofoftech/fleet-control` package from a trusted control plane, never from tenant request scope. The package is published, so enforce that boundary in your own build: see [Import it only from a trusted control plane](../packages/fleet-control/README.md#import-it-only-from-a-trusted-control-plane).

## Choose a provisioning backend

Both backends implement the same ordered `ProvisioningBackend` contract:

| Backend | Accepted artifacts | Deployment mechanism |
| --- | --- | --- |
| `WranglerLoopBackend` | Platform-authored only | Wrangler 4 commands and generated configuration |
| `WorkersForPlatformsBackend` | Platform-authored catalogs and external project releases | Cloudflare Upload API in an untrusted dispatch namespace |

`WorkersForPlatformsBackend` requires its untrusted dispatch namespace, named shared outbound Worker, and state-egress root secret at construction. It rejects an incomplete configuration before provider access. Normal provisioning always places trusted per-deployment state in that dispatch namespace. Ordinary state scripts exist only as already-persisted bridges managed by the dedicated backend-switch lifecycle.

The plain backend rejects external artifacts before creating a resource. Switch to Workers for Platforms before you run the first customer-authored artifact. Set `routeHostname` to the customer-facing custom domain. For plain Workers, set `maintenanceBaseUrl` to the distinct Workers control origin; for Workers for Platforms, set it to the control-plane dispatcher origin. Fleet state reserves the route before any Worker can publish it.

## Provision a deployment

Create a backend, durable `FleetStateStore`, validated `DeploymentSpec`, and distinct credentials. `provisionDeployment()` applies this order:

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
});

if (!result.maintenance.armed) {
  throw new Error('maintenance did not arm');
}
```

The function persists every completed phase and validates the immutable tenant, environment, logical script, database, and route mapping on retry. Database creation advances from `database-reserved` to an explicit create-authorized ownership phase before the provider mutation, so a same-name discovery cannot be silently adopted after a crash or race. `D1FleetStateStore` serializes each deployment lifecycle with a renewable database-time lease and fences state writes with its owner token. It commits the exact Worker, dispatch-script, and R2 claims with the fleet row in one D1 batch. The final statement fails the whole batch if the lease expired after an earlier claim statement. A lost batch response converges only after raw serialized row values, the complete owned claim set, desired-key occupants, and the live lease all match. Specification digests make a retry use the exact intended modules, bindings, limits, and migrations. A changed ready specification must go through `migrateFleet()`.

Provisioning resumes from its last durable phase without repeating a committed step. Before `ready`, it compares the exact live tenant, environment, D1 binding, schema, specification digest, Durable Object bindings, plain-text variables, and secret names. Plain Worker, dispatch Worker, backend-switch, and control-plane inspection must consume every raw provider binding entry. An unknown type, malformed entry, duplicate name, binding absent from the structured inspection, or missing complete inventory fails closed even when the desired application groups are empty. Ordinary Worker secret names come from the authoritative secret-list API; if version resources also report them, the two inventories must agree. A failed first create revokes credentials and removes resources created by that attempt. If an upload may have succeeded, cleanup treats the Worker as present until deletion is positively confirmed; D1 is never deleted while a Worker or route may remain. Cleanup errors remain attached to `ProvisioningError.cleanupErrors`, and the durable phase remains available to retry or to `cleanupDeploymentArtifacts()`.

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

`WranglerLoopBackend` uploads a digest-tagged Worker Version with the exact built-in and declared application secret set in a mode-0600 file. For an existing deployment, it attaches the candidate at zero percent, sends the maintenance request with `Cloudflare-Workers-Version-Overrides`, and accepts the response only when `deploymentSpecDigest` matches the requested build. It then promotes that version to 100 percent, verifies the live custom-domain owner against `PromotionGuard`, attaches the domain through the explicit Cloudflare API seam, and re-inspects the mapping. A failed maintenance check never publishes the route.

An initial Worker that introduces Durable Object classes uses a route-free `wrangler deploy`, validates maintenance through its distinct control origin, and publishes the customer domain afterward. An existing plain Worker cannot stage a new Durable Object lifecycle migration through Workers Versions; perform that change at an explicit immediate-deployment maintenance boundary. No generated configuration contains a cron trigger.

The Wrangler backend uses the direct Cloudflare D1 query and batch APIs for database ownership and application migrations, inside the same external mutation-fence boundary as its other provider writes. SQL and ordered scalar bindings remain separate until D1 parses them. Anonymous `?` and numbered `?NNN` parameters therefore coexist safely with literal question marks, escaped strings, quoted identifiers, and comments. D1 does not currently support named SQLite parameters. Each migration and its ledger row execute in one provider-native atomic batch.

Every generated plain Worker uses a platform-owned guarded entry module. The guard recognizes only the normalized `routeHostname` and `maintenanceBaseUrl` hosts. The route host rejects `Cloudflare-Workers-Version-Overrides` before it invokes application code. The control host accepts only `POST /admin/ensure-maintenance` and `GET /admin/maintenance-status`, and it requires the exact maintenance administrator bearer secret.

Every other hostname returns HTTP 404. This includes `workers.dev` unless `maintenanceBaseUrl` names that exact host. Preview URLs remain disabled.

The guard re-exports local Durable Object classes and invokes the original default object’s `fetch` method with its original receiver. The main module must be importable string JavaScript whose evaluated default export is an object with a callable `fetch`; module evaluation fails closed otherwise. A versioned binding prevents a same-spec version created before this guard existed from satisfying candidate convergence.

## Promote and roll back external releases

Workers for Platforms user Workers have no public version-selection or gradual-deployment API. A same-name upload immediately replaces the live script at 100 percent. Fleet control therefore treats `DeploymentSpec.scriptName` as a logical project name and derives a distinct, content-addressed physical script name for every external specification.

`migrateFleet()` persists an exact target before it mutates D1 or any Worker. The target covers the candidate specification, prior and target releases, trusted state artifact and state-egress credential digests, Durable Object migration history, complete D1 migration history, schema version, outbound policy, audit queue, and maintenance verifier. The operation then records `schema-applied`, `platform-applied`, `candidate-deployed`, `candidate-armed`, and `route-published` subphases. A retry compares the requested target with that durable intent and resumes from the last completed boundary. A platform-only migration reuses the active release without creating a duplicate rollback snapshot. It never recomputes a changed profile into an in-progress migration.

Before any migration mutation, the pending release owns its canonical application variables, secret descriptors, and resolved R2 mapping independently of the deployment-wide active projection. After the D1 expand migration commits, `migrateFleet()` reconciles the trusted state script, uploads and validates the candidate against that pending topology without changing customer traffic, arms it through the maintenance-only dispatcher path, and publishes the host mapping. The prior physical script remains registered with its own application topology as the rollback release. `rollbackExternalRelease()` requires the exact retained specification digest, records rollback intent before changing the route, validates the retained script against the retained topology, and restores the deployment-wide application projection when it swaps the active and rollback release snapshots after publication. The fleet record keeps the applied D1 schema version monotonic and stores each release's supported schema separately, so rolling traffic back never claims that an applied migration was undone.

Host publication uses Workers KV because the dispatch Worker needs a low-latency hostname lookup. The same canonical host record supplies policy context and the SHA-256 state-egress credential digest to the one shared outbound Worker. The state script calls only its named `StateEgress` entrypoint. The shared outbound Worker verifies the exact tenant, environment, resource group, state script, route, policy, and credential before origin fetch. It strips the reserved context headers before sending the request. Every Workers for Platforms fleet record persists the canonical policy independently of a release. External deployments use the organization allowlist from their trusted profile, while platform-authored deployments default to a tenant-bound deny-all policy. Promotion and rollback publish that persisted policy, and drift recomputes and compares its identity, hosts, and digest. Workers KV is eventually consistent, so the durable fleet record and deployment lease remain the ownership authority. During propagation, requests may reach either retained compatible release. Do not delete the prior script until a later successful release retires it.

The state-egress credential digest is immutable for an existing trusted state resource. Rotating the root secret requires a coordinated credential migration that updates the state Worker secret and canonical host target as one durable lifecycle. The current provider can inspect the exact secret-name set, but Cloudflare does not expose secret values for attestation. Fleet control therefore rejects an implicit digest change before provider mutation.

External candidates cannot own Durable Object classes or migrations, and an external specification cannot choose a state script, dispatch namespace, outbound target, or physical R2 bucket name. The trusted control plane resolves every requested Durable Object binding to the deployment's stable state script and every R2 descriptor to the fleet-owned deployment resource. The state script owns FlowSafe runs, approvals, alarms, and Durable Object code while candidates remain independently replaceable. The state script receives the named `OUTBOUND_PROXY` service binding, its context-bound credential, the audit queue, and the maintenance secret. The candidate receives none of them. If audit export is enabled, its `AUDIT_PROXY` binding is a remote Durable Object binding to `FlowsafeFleetAuditProxy` in the exact state script. Fresh state scripts include the dispatch namespace in that binding; adopted ordinary bridge scripts omit it. Fleet state persists the exact state binding inventory and an append-only snapshot of every authoritative namespace ID. Drift and teardown use that snapshot rather than recomputing names. Every D1 migration introduced while a rollback release is retained must be explicitly marked rollback-compatible and use expand-only schema changes. Apply contract changes only after the rollback window closes.

Supply a backend-owned `platformProfileFor(spec)` provider when the Workers for Platforms backend can receive external artifacts. The provider returns the trusted state artifact, platform state migration history, organization egress allowlist, and optional legacy bridge template. Fleet control validates and hashes that profile before upload. A profile or policy change requires `migrateFleet()`, even when the customer candidate bytes are unchanged. Customer modules are never reused in fresh trusted state. The stable state name derives from immutable deployment identity, not candidate contents, so release promotion and rollback do not replace platform state.

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

`decommissionBackendSwitch()` handles an operator-requested teardown from every switch subphase and after later migration or rollback. Before removing traffic, it persists the current desired digest, the canonical set of host targets allowed by the durable lifecycle phase, ordinary bridge identity, effective bridge plan, application R2 resources, and the exact union of active, pending, migration-prior, rollback, retiring, and original switch releases. Migration permits the prior route until the candidate is armed, both prior and target routes at the publication ambiguity boundary, and only the target after publication. Publishing permits its intended pending release. Rollback and teardown preserve both active and pending routes until the final ready state commits. Each route target is bound to its snapshotted physical release and platform target. Fleet control accepts only a byte-exact member of that set when removing `HOSTS`. Each release carries its own application and physical binding topology. Fleet control records delete authorization and positive absence for each release before it deletes the bridge and verifies namespace removal. For each application R2 bucket it persists detach authorization, detached confirmation, empty authorization, empty confirmation, delete authorization, and positive absence. A retry resumes from the individual release or bucket record before D1 export and deletion. The same fleet lease fences every phase.

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

External resource-group inventory checks the dispatch-native state script and each candidate as distinct roles under one immutable group identity. During a legacy switch rollback window, it also checks the adopted ordinary bridge. It verifies the shared D1 binding, local state namespaces, candidate remote Durable Object targets, application variables, exact secret names, application R2 bindings, exact named service and queue topology, trusted artifact and policy digests, static tenant and environment attribution, and the absence of public state routes. The backend-owned shared audit queue name is part of the persisted platform target and resource snapshot, so configuration drift cannot retarget an existing deployment. Route inventory records whether each entry came from the host registry, a custom domain, or a zone route. Fleet control reserves the owner-checked registry entry before upload so every script created through the supported client is enumerable by name. Plain-only collection makes no dispatch-namespace request.

Pass that independently collected `FleetResourceInventory` to `auditFleetDrift()`; do not derive it from fleet records. The audit works in both directions and contains an individual inspection or watchdog error so one broken deployment does not hide the rest. It reports missing, duplicate, malformed, and orphan scripts, databases, routes, Durable Object namespaces, and R2 buckets. It also reports exact application-variable, secret-name, R2-binding, route, artifact, and schema drift. Lifecycle-aware expectations distinguish an unpublished candidate, a published deployment, a retained rollback release, and resources that should already be absent during decommissioning. Secret-value drift remains opaque because provider inventory cannot return or hash the stored value.

The maintenance watchdog evaluates sweep, purge, and the optional background tick independently, including their last attempt and error. Plain, platform-authored Workers authenticate maintenance with the deployment's maintenance secret. An external release never receives that reusable secret. Fleet control instead signs a short-lived Ed25519 capability bound to the operation, tenant, environment, physical release script, specification digest, expiry, and nonce. The global dispatcher verifies that capability before calling `DISPATCH.get()`, and the trusted state Worker verifies it again against static deployment bindings. `ensure-maintenance` atomically consumes the nonce, while status remains replay-safe and read-only. The state Worker signs the exact result with its per-state HMAC secret, and fleet control ignores the candidate's unsigned body. The mutation request timeout must remain shorter than both the capability lifetime and the active mutation lease. The current verifier is intentionally immutable across an existing global dispatcher and deployment record: ordinary per-tenant key rotation is unsupported. Rotation requires a coordinated fleet maintenance migration or a future overlapping JWKS design.

## Decommission without losing the export

`decommissionDeployment()` persists each destructive phase:

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

Zero ingress covers every surface owned by the workload. Plain Workers must have no custom domain or zone route, and fleet control explicitly disables and rechecks workers.dev and preview URLs. Workers for Platforms deployments must have no `HOSTS` record. A backend switch checks `HOSTS` plus every ordinary bridge ingress surface. If ingress drift or a late R2 write appears after removal, fleet control preserves the traffic-removed state and every script, credential, platform resource, and bucket. It does not restore traffic. Remove the unexpected ingress or evacuate the bucket directly, then retry.

An attached or nonempty application R2 bucket blocks deletion. Fleet control never purges application objects automatically because no generic export format can preserve application semantics. The backend requires positive persisted ownership, exact provider identity, complete paginated emptiness inspection, and positive absence after deletion.

An export failure, empty export, size mismatch, or missing integrity metadata prevents database deletion. Both provider backends require a `DurableDatabaseExportStore`; the store must return the byte count and SHA-256 of the committed artifact, which the backend compares with the exported stream. The Wrangler backend writes a mode-0600 scratch file, streams it to the durable store, and removes the scratch file before returning only the store location. `FileSystemDatabaseExportStore` computes that integrity while atomically publishing a verified local artifact for the credentialed scratch-account gate. Its configured filesystem is responsible for the resulting artifact's durability; production control planes should implement the same narrow interface with durable object storage such as R2. Cleanup and decommissioning resolve the database by its persisted immutable ID, validate its name and deployment sentinel when present, and treat only a positive not-found result as absence. Before export and again before deletion, the backend scans every version in each current ordinary Worker deployment, including request-addressable zero-percent candidates, and every dispatch script account-wide for an exact D1 attachment; an unregistered or unrelated attachment fails closed. Historical versions outside the current deployment are excluded. A retry therefore converges after a crash between database deletion and the final state write without following a reused name to another database. Decommissioning retains the persisted platform-resource snapshot until both trusted Workers are positively absent, and never deletes D1 while a state Worker can still hold its binding.

Every backend operation that can issue a non-GET Cloudflare request receives the active `ExternalMutationFence`. `CloudflareProvisioningClient.withMutationFence()` carries that fence across queued SDK calls and renews ownership immediately before each provider write. The client rejects unfenced writes and request timeouts that are not shorter than the lease lifetime. The Wrangler backend applies the same rule to its process-tree timeout and rejects an invalid composition before spawning the mutation command. A lease takeover therefore stops the next provider mutation, including retries and cleanup sub-steps.

`D1FleetStateStore` memoizes only a successful or currently in-flight schema bootstrap. A transient D1 failure clears the memo so the same store instance can retry. The additive `backend_switch_intent` upgrade verifies the exact resulting nullable `TEXT` column and accepts only a verified duplicate-column race between concurrent cold replicas; every other migration failure propagates.

Cloudflare documents a 1,200-request-per-five-minute client API limit per user or account token, cumulative across callers. `CloudflareProvisioningClient` therefore requires a `CloudflareApiRateCoordinator`. Production uses `D1CloudflareApiRateCoordinator` over one shared direct Workers `D1Database` binding and an explicit nonsecret `quotaScope` identifying that provider quota. The coordinator calls only the binding's `prepare()` and `batch()` methods. Its runtime guard rejects objects without that interface, but JavaScript cannot prove whether a structurally compatible object is a direct binding or a remote facade. The trusted host must enforce the direct-binding requirement because remote coordination queries would consume the same Client API quota being protected. Independently constructed coordinators with the same binding and scope atomically share a rolling cap of 1,100 requests per five minutes across replicas. A non-Worker control plane must use a separately deployed coordinator service rather than a remote database adapter. Every SDK request, retry, pagination page, authenticated manual request, and signed export download reserves capacity before the network request. Coordinator failure fails closed. Ambiguous transport failures retain their reservation. Never derive the scope from, persist, or log the raw token. User-scoped credentials used across accounts must deliberately reuse one scope. The remaining 100 requests are reserve for dashboard and other out-of-band traffic; fleet control cannot count or guarantee room for that traffic.

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
- `CLOUDFLARE_API_TOKEN` with API Tokens Read, Zone Read, Workers Routes Read, and Workers Routes Write for every zone in the account
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
